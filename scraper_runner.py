"""
Robust scraper-subprocess management for the dashboard (no Streamlit imports, so it is testable).

What it provides
  * run_subprocess(): runs the scraper with a MAX RUNTIME and a STALL (no-output) timeout, streams
    its output line by line, and ALWAYS cleans up: it asks the scraper to stop (so it can save partial
    results and close its browser), then kills the whole process group so no orphaned
    Chromium/Playwright processes survive.
  * JobRegistry: at most one running scrape per user and a global concurrency cap (protects the
    server's memory and the shared browser profile).
  * new_output_path(): a unique CSV path per user and per job, so concurrent users can never read
    or overwrite each other's files; prune_outputs() keeps old files from piling up.
  * scraper_env(): environment for the child process with secret-looking variables removed.
"""

import hashlib
import os
import queue
import re
import signal
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Mapping, Optional, Tuple

# --------------------------------------------------------------------------- #
# Timeouts
# --------------------------------------------------------------------------- #
BASE_RUNTIME_SECONDS = 120          # browser start-up, first page, shutdown
PER_PAGE_BUDGET_SECONDS = 120       # one results page: load + scrolling + pauses + retries
DEFAULT_MAX_RUNTIME_CAP = 3600      # absolute ceiling for one job (override: SCRAPER_MAX_RUNTIME_SECONDS)
DEFAULT_STALL_TIMEOUT = 600         # no output at all for this long = hung (SCRAPER_STALL_TIMEOUT_SECONDS)
TERMINATE_GRACE_SECONDS = 15        # time the scraper gets to save partial results after a stop request


def _env_int(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, "").strip())
        return value if value > 0 else default
    except ValueError:
        return default


def compute_timeouts(num_keywords: int, max_pages: int) -> Tuple[int, int]:
    """(max_runtime_seconds, stall_timeout_seconds), scaled to the size of the job but capped."""
    cap = _env_int("SCRAPER_MAX_RUNTIME_SECONDS", DEFAULT_MAX_RUNTIME_CAP)
    stall = _env_int("SCRAPER_STALL_TIMEOUT_SECONDS", DEFAULT_STALL_TIMEOUT)
    pages = max(1, int(num_keywords)) * max(1, int(max_pages))
    return min(cap, BASE_RUNTIME_SECONDS + pages * PER_PAGE_BUDGET_SECONDS), stall


# --------------------------------------------------------------------------- #
# Process control
# --------------------------------------------------------------------------- #
@dataclass
class RunResult:
    status: str                     # "completed" | "failed" | "timeout" | "stalled"
    return_code: Optional[int]
    lines: List[str] = field(default_factory=list)
    elapsed: float = 0.0
    message: str = ""

    @property
    def ok(self) -> bool:
        return self.status == "completed"


def _popen_kwargs() -> Dict[str, object]:
    """Start the scraper in its own process group/session so the whole tree can be stopped."""
    if os.name == "posix":
        return {"start_new_session": True}
    return {"creationflags": getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)}


def _request_graceful_stop(proc: subprocess.Popen) -> None:
    try:
        if os.name == "posix":
            os.kill(proc.pid, signal.SIGTERM)
        elif hasattr(signal, "CTRL_BREAK_EVENT"):
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.terminate()
    except (ProcessLookupError, PermissionError, OSError):
        pass


def _kill_tree(proc: subprocess.Popen) -> None:
    """Hard-kill the scraper AND everything it started (Playwright driver, Chromium)."""
    try:
        if os.name == "posix":
            os.killpg(proc.pid, signal.SIGKILL)   # the group id equals the leader's pid (new session)
        else:
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                           capture_output=True, timeout=20)
    except (ProcessLookupError, PermissionError, OSError, subprocess.SubprocessError):
        pass


def stop_process_tree(proc: subprocess.Popen, grace: float = TERMINATE_GRACE_SECONDS) -> None:
    """
    Stop the scraper and everything it started. Safe to call at any time, including when it has
    already exited (it then only sweeps up leftover child processes).
      1. polite request (SIGTERM / Ctrl-Break): the scraper saves partial results and closes Chromium;
      2. after `grace` seconds, SIGKILL / taskkill the whole process tree.
    """
    if proc.poll() is None:
        _request_graceful_stop(proc)
        try:
            proc.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            pass
    _kill_tree(proc)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass


def run_subprocess(cmd: List[str], *, cwd: str, env: Mapping[str, str],
                   max_runtime: float, stall_timeout: float,
                   on_line: Optional[Callable[[str], None]] = None,
                   max_kept_lines: int = 3000, poll_interval: float = 1.0,
                   grace: float = TERMINATE_GRACE_SECONDS) -> RunResult:
    """
    Run `cmd`, stream its output to `on_line`, and enforce the runtime and stall limits.

    Output is read on a helper thread and handed over through a queue, so this function (and your
    `on_line` callback, which may call Streamlit) always runs on the caller's thread. If `on_line`
    raises (for example Streamlit interrupting a script run), the subprocess is still cleaned up
    before the exception propagates.
    """
    proc = subprocess.Popen(
        cmd, cwd=cwd, env=dict(env),
        stdin=subprocess.DEVNULL,                       # non-interactive: nobody can answer prompts
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace", bufsize=1,
        **_popen_kwargs(),
    )
    lines_q: "queue.Queue[Optional[str]]" = queue.Queue()

    def pump() -> None:
        try:
            assert proc.stdout is not None
            for raw in proc.stdout:
                lines_q.put(raw)
        except Exception:
            pass
        finally:
            lines_q.put(None)                           # end-of-output marker

    reader = threading.Thread(target=pump, name="scraper-output", daemon=True)
    reader.start()

    lines: List[str] = []

    def keep(raw: str) -> Optional[str]:
        line = raw.rstrip()
        if line:
            lines.append(line)
            if len(lines) > max_kept_lines:
                del lines[: len(lines) - max_kept_lines]
        return line or None

    started = last_output = time.monotonic()
    status, message = "completed", ""
    try:
        while True:
            try:
                raw: Optional[str] = lines_q.get(timeout=poll_interval)
            except queue.Empty:
                raw = ""                                # nothing new yet: just re-check the clocks
            if raw is None:                             # the scraper closed its output
                break
            now = time.monotonic()
            if raw:
                last_output = now
                line = keep(raw)
                if line and on_line:
                    on_line(line)
            if now - started > max_runtime:
                status = "timeout"
                message = f"exceeded the maximum run time of {int(max_runtime // 60)} min"
                break
            if now - last_output > stall_timeout:
                status = "stalled"
                message = f"no output for {int(stall_timeout // 60)} min (the scraper looks hung)"
                break

        if status == "completed":
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                status, message = "failed", "the scraper closed its output but did not exit"
    finally:
        stop_process_tree(proc, grace)                  # no-op if it exited; sweeps leftover children
        reader.join(timeout=3)
        try:
            if proc.stdout:
                proc.stdout.close()
        except Exception:
            pass
        while True:                                     # capture the scraper's last words (e.g. "Interrupted")
            try:
                raw = lines_q.get_nowait()
            except queue.Empty:
                break
            if raw:
                keep(raw)

    if status == "completed" and proc.returncode != 0:
        status, message = "failed", f"the scraper exited with code {proc.returncode}"
    return RunResult(status=status, return_code=proc.returncode, lines=lines,
                     elapsed=time.monotonic() - started, message=message)


def scraper_env(parent: Optional[Mapping[str, str]] = None) -> Dict[str, str]:
    """
    Environment for the child process: the parent's, minus anything that looks like a credential
    (the scraper does not need Supabase/Stripe/app secrets). SHOPEE_* settings are always kept.
    """
    secretish = re.compile(r"(SECRET|TOKEN|PASSWORD|PASSWD|CREDENTIAL|API_?KEY|PRIVATE|(^|_)KEY($|_))", re.I)
    blocked_prefixes = ("SUPABASE_", "STRIPE_", "APP_")
    env: Dict[str, str] = {}
    for name, value in (parent if parent is not None else os.environ).items():
        upper = name.upper()
        if upper.startswith("SHOPEE_"):
            env[name] = value
        elif upper.startswith(blocked_prefixes) or secretish.search(upper):
            continue
        else:
            env[name] = value
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"                   # Thai text on Windows consoles/pipes
    return env


# --------------------------------------------------------------------------- #
# Concurrency: one job per user, and a global cap
# --------------------------------------------------------------------------- #
class JobRegistry:
    """
    Tracks running scrapes. It lives in this module, which Python imports once per server process,
    so all browser sessions share it (a Streamlit page script itself is re-run on every interaction).
    With several server replicas each one has its own registry.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active: Dict[str, float] = {}             # user id -> start time (monotonic)

    def acquire(self, user_id: str, max_concurrent: int, stale_after: float = 4500) -> Optional[str]:
        """Reserve a slot. Returns None on success, or a message explaining why not."""
        with self._lock:
            now = time.monotonic()
            for uid, started in list(self._active.items()):   # reclaim slots leaked by a crashed run
                if now - started > stale_after:
                    del self._active[uid]
            if user_id in self._active:
                return "You already have a scrape running (maybe in another tab). Wait for it to finish."
            if len(self._active) >= max_concurrent:
                return (f"The server is busy ({len(self._active)} scrape(s) running, limit "
                        f"{max_concurrent}). Please try again in a minute.")
            self._active[user_id] = now
            return None

    def release(self, user_id: str) -> None:
        with self._lock:
            self._active.pop(user_id, None)

    def active_count(self) -> int:
        with self._lock:
            return len(self._active)


JOBS = JobRegistry()


# --------------------------------------------------------------------------- #
# Per-user output files
# --------------------------------------------------------------------------- #
def safe_user_dirname(user_id: str) -> str:
    """A filesystem-safe directory name for a user: the UUID itself, else a hash (never raw input)."""
    try:
        return str(uuid.UUID(str(user_id)))
    except (ValueError, AttributeError, TypeError):
        return "u_" + hashlib.sha256(str(user_id).encode("utf-8")).hexdigest()[:24]


def new_output_path(base_dir: Path, user_id: str) -> Path:
    """
    A unique CSV path inside <base_dir>/<user>/ for one job. Different users never share a folder and
    two jobs never share a file name, even when started in the same second.
    """
    base = Path(base_dir).resolve()
    user_dir = base / safe_user_dirname(user_id)
    user_dir.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(user_dir, 0o700)                       # best effort: owner-only on POSIX
    except OSError:
        pass
    name = f"shopee_products_{time.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.csv"
    path = (user_dir / name).resolve()
    if base not in path.parents:                        # defence in depth against path tricks
        raise ValueError("output path escaped the output directory")
    return path


def prune_outputs(user_dir: Path, keep: int = 5, max_age_seconds: float = 24 * 3600) -> int:
    """Delete a user's old CSVs: older than `max_age_seconds`, or beyond the newest `keep`."""
    removed = 0
    try:
        files = sorted((p for p in Path(user_dir).glob("shopee_products_*.csv") if p.is_file()),
                       key=lambda p: p.stat().st_mtime, reverse=True)
    except OSError:
        return 0
    cutoff = time.time() - max_age_seconds
    for index, path in enumerate(files):
        try:
            if index >= keep or path.stat().st_mtime < cutoff:
                path.unlink()
                removed += 1
        except OSError:
            pass
    return removed


def discard_if_empty(path: Path) -> None:
    """Remove a job's output file if it holds no data (header-only or empty)."""
    try:
        if path.exists() and path.stat().st_size <= 3:   # empty, or just a UTF-8 BOM
            path.unlink()
    except OSError:
        pass
