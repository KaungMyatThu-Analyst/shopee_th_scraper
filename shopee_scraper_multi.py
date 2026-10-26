#!/usr/bin/env python3
"""
Shopee Thailand multi-keyword scraper with pagination (Playwright, sync API,
persistent browser profile).

For every keyword it walks the search result pages (?keyword=...&page=0,1,2,...), scrolls each
page to trigger the lazy-loaded product API, captures `api/v4/search/search_items` responses,
parses them with a schema-agnostic extractor, de-duplicates products across ALL keywords and
pages, and writes everything to one combined CSV (UTF-8 with BOM so Excel shows Thai text).

The browser profile (./shopee_profile) keeps your login/captcha session between runs. If
Shopee blocks a page (e.g. error 90309999), the script pauses so you can fix it by hand.

Usage:
    pip install playwright && playwright install chromium
    python shopee_scraper_multi.py
    python shopee_scraper_multi.py --keywords skincare makeup sunscreen --max-pages 5
    python shopee_scraper_multi.py --keywords serum "ครีมกันแดด" --output results.csv
    python shopee_scraper_multi.py --channel chrome        # use installed Google Chrome

Log in once by hand on your own computer (session is saved to the profile):
    python shopee_scraper_multi.py --login
    python shopee_scraper_multi.py --login --storage-state shopee_state.json   # also export for a server

Runs on your own computer AND on a headless Linux server such as Streamlit Community Cloud:
  * Desktop: visible browser + persistent profile (./shopee_profile), so logins/captchas stick.
    Close any other browser window that uses the same profile folder before running.
  * Server/cloud: headless browser, container-safe Chromium flags (--no-sandbox,
    --disable-dev-shm-usage, --disable-gpu) and a standard non-persistent context, so profile
    locks can never break the launch. Detection is automatic; override with flags or env vars:

      --headless / --headed          SHOPEE_HEADLESS=1|0
      --ephemeral / --persistent     SHOPEE_EPHEMERAL=1|0
      --storage-state FILE           SHOPEE_STORAGE_STATE=FILE  or  SHOPEE_STORAGE_STATE_JSON='{...}'
      (force cloud detection)        SHOPEE_CLOUD=1|0
"""

import argparse
import csv
import json
import os
import random
import re
import signal
import subprocess
import sys
import time
from collections import deque
from dataclasses import dataclass, replace as dc_replace
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote

from playwright.sync_api import (
    BrowserContext,
    Page,
    Response,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
BASE_URL = "https://shopee.co.th"
API_MARKER = "api/v4/search/search_items"

DEFAULT_KEYWORDS = ["skincare", "makeup", "sunscreen"]
DEFAULT_MAX_PAGES = 3                       # pages per keyword (Shopee shows ~60 items/page)
DEFAULT_OUTPUT_CSV = "shopee_multi_keyword_products.csv"
DEFAULT_PROFILE_DIR = "./shopee_profile"
DEBUG_DIR = "shopee_debug"

PAGE_DELAY = (3.0, 6.0)                     # seconds to wait between pages
KEYWORD_DELAY = (8.0, 15.0)                 # seconds to wait between keywords
KEYWORD_SEP = "|"                           # separator used in the CSV "keywords" column

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
VIEWPORT = {"width": 1366, "height": 850}
LOCALE = "th-TH"
TIMEZONE = "Asia/Bangkok"

LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-infobars",
    "--no-first-run",
    "--no-default-browser-check",
]

# Extra flags for containers / headless servers (Streamlit Cloud, Docker, CI):
#   --no-sandbox              the Chromium sandbox needs kernel features containers usually lack
#   --disable-dev-shm-usage   /dev/shm is tiny in containers; use /tmp instead (prevents crashes)
#   --disable-gpu             no GPU on servers
SERVER_LAUNCH_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
]

STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'languages', { get: () => ['th-TH', 'th', 'en-US', 'en'] });
window.chrome = window.chrome || { runtime: {} };
"""

# Shopee stores prices in micro-units: 129.00 THB -> 12_900_000 (x 100,000).
PRICE_DIVISOR = 100_000
MICRO_UNIT_THRESHOLD = 100_000

# Candidate keys, compared in normalized form (lowercase letters/digits only), so
# "item_id", "itemId" and "itemid" all match "itemid".
ITEM_ID_KEYS = ["itemid"]
SHOP_ID_KEYS = ["shopid"]
NAME_KEYS = ["name", "title", "itemname", "productname"]
PRICE_KEYS = ["price", "pricemin", "displayprice", "pricebeforediscount", "pricemax"]
STOCK_KEYS = ["stock", "normalstock", "currentstock"]
SOLD_KEYS = ["historicalsold", "historicalsoldcount", "sold", "globalsold", "soldcount"]

# Wrapper blocks Shopee uses around the real product fields (normalized names).
WRAPPER_KEYS = {
    "itembasic", "itemdata", "itemcard", "itemcarddisplayedasset",
    "itemcarddisplayprice", "itemcarddisplaysoldcount", "itemcardflags",
}


# --------------------------------------------------------------------------- #
# Generic helpers
# --------------------------------------------------------------------------- #
def norm_key(key: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(key).lower())


def is_scalar(value: Any) -> bool:
    return value is not None and not isinstance(value, (dict, list))


def is_id_like(value: Any) -> bool:
    """True for positive ints or digit-only strings (not bool)."""
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return value > 0
    if isinstance(value, float):
        return value > 0 and value == int(value)
    if isinstance(value, str):
        return value.isdigit() and int(value) > 0
    return False


def to_int(raw: Any) -> Optional[int]:
    """Parse ints from numbers or strings such as '1,234', '1.2k', '1.2พัน', '3หมื่น'."""
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        return int(raw)
    text = str(raw).strip().replace(",", "")
    match = re.match(r"^(-?\d+(?:\.\d+)?)\s*(k|K|พัน|หมื่น|แสน|m|M|ล้าน)?", text)
    if not match:
        return None
    mult = {"k": 1e3, "K": 1e3, "พัน": 1e3, "หมื่น": 1e4, "แสน": 1e5,
            "m": 1e6, "M": 1e6, "ล้าน": 1e6}.get(match.group(2) or "", 1)
    return int(float(match.group(1)) * mult)


def normalize_price(raw: Any) -> Optional[float]:
    """Convert a raw Shopee price to THB; values >= 100,000 are treated as micro-units."""
    if raw is None or isinstance(raw, bool):
        return None
    try:
        value = float(str(raw).replace(",", "")) if isinstance(raw, str) else float(raw)
    except (TypeError, ValueError):
        return None
    if value >= MICRO_UNIT_THRESHOLD:
        value /= PRICE_DIVISOR
    return round(value, 2)


def slugify(text: str) -> str:
    return re.sub(r"[^\w]+", "_", text).strip("_") or "keyword"


# --------------------------------------------------------------------------- #
# Schema-agnostic product discovery and extraction
# --------------------------------------------------------------------------- #
def is_product(node: Dict[str, Any]) -> bool:
    """A dict is a product (or wrapper) if it directly holds a numeric item id."""
    return any(norm_key(k) in ITEM_ID_KEYS and is_id_like(v) for k, v in node.items())


def collect_product_dicts(node: Any, found: List[Dict[str, Any]], depth: int = 0) -> None:
    """
    Walk arbitrary JSON and gather product dicts. Handles data.items, items, item_list,
    numeric-string keys ({"0": {...}, "1": {...}}) and wrapped objects (item_basic, item_data).
    """
    if depth > 15:
        return
    if isinstance(node, dict):
        if is_product(node):
            found.append(node)
            return
        for value in node.values():
            if isinstance(value, (dict, list)):
                collect_product_dicts(value, found, depth + 1)
    elif isinstance(node, list):
        for value in node:
            if isinstance(value, (dict, list)):
                collect_product_dicts(value, found, depth + 1)


def build_scopes(product: Dict[str, Any]) -> List[Dict[str, Any]]:
    """[product, known wrapper children...] as {normalized_key: scalar}; top level wins."""
    scopes: List[Dict[str, Any]] = []
    queue = deque([(product, 0)])
    while queue:
        node, depth = queue.popleft()
        scopes.append({norm_key(k): v for k, v in node.items() if is_scalar(v)})
        if depth >= 3:
            continue
        for k, v in node.items():
            if isinstance(v, dict) and norm_key(k) in WRAPPER_KEYS:
                queue.append((v, depth + 1))
    return scopes


def deep_fallback(product: Dict[str, Any], keys: List[str], max_depth: int = 5) -> Any:
    """Last resort: BFS through all nested dicts for the first matching scalar."""
    queue = deque([(product, 0)])
    while queue:
        node, depth = queue.popleft()
        lookup = {norm_key(k): v for k, v in node.items() if is_scalar(v)}
        for key in keys:
            if key in lookup:
                return lookup[key]
        if depth < max_depth:
            queue.extend((v, depth + 1) for v in node.values() if isinstance(v, dict))
    return None


def get_field(product: Dict[str, Any], scopes: List[Dict[str, Any]], keys: List[str]) -> Any:
    for scope in scopes:
        for key in keys:
            if scope.get(key) is not None:
                return scope[key]
    return deep_fallback(product, keys)


def parse_product(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        scopes = build_scopes(raw)
        item_id = get_field(raw, scopes, ITEM_ID_KEYS)
        if not is_id_like(item_id):
            return None
        shop_id = get_field(raw, scopes, SHOP_ID_KEYS)
        return {
            "item_id": int(item_id),
            "shop_id": int(shop_id) if is_id_like(shop_id) else None,
            "name": get_field(raw, scopes, NAME_KEYS),
            "price": normalize_price(get_field(raw, scopes, PRICE_KEYS)),
            "stock": to_int(get_field(raw, scopes, STOCK_KEYS)),
            "historical_sold": to_int(get_field(raw, scopes, SOLD_KEYS)),
        }
    except Exception as exc:  # one bad product must never crash the run
        print(f"[warn] Skipping malformed product: {exc!r}", file=sys.stderr)
        return None


def parse_payload(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, str):  # double-encoded JSON
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return []
    raw_products: List[Dict[str, Any]] = []
    collect_product_dicts(payload, raw_products)
    return [p for p in (parse_product(r) for r in raw_products) if p]


def dedupe(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Simple de-duplication of one list by (shop_id, item_id)."""
    seen, unique = set(), []
    for p in products:
        key = (p.get("shop_id"), p.get("item_id"))
        if key not in seen:
            seen.add(key)
            unique.append(p)
    return unique


def block_error(payload: Any) -> Optional[Any]:
    """Return the API error code if the payload looks like a block (e.g. 90309999)."""
    if isinstance(payload, dict):
        err = payload.get("error")
        if err not in (None, 0, "0", ""):
            return err
    return None


# --------------------------------------------------------------------------- #
# Global store: de-duplication across all keywords and pages
# --------------------------------------------------------------------------- #
Key = Tuple[Optional[int], int]


class ProductStore:
    """
    Collects products from every keyword/page and removes duplicates.

    * Identity is (shop_id, item_id); if shop_id is missing in one capture, the product is
      matched by item_id so it is not stored twice.
    * When the same product shows up again, missing fields are filled in and the keyword is
      appended to its `keywords` column (e.g. "skincare|sunscreen").
    """

    FILL_FIELDS = ("shop_id", "name", "price", "stock", "historical_sold")

    def __init__(self) -> None:
        self._items: Dict[Key, Dict[str, Any]] = {}
        self._by_item: Dict[int, Key] = {}
        self._kw_seen: Dict[str, Set[Key]] = {}
        self.raw_count = 0  # products seen before de-duplication

    def __len__(self) -> int:
        return len(self._items)

    def _resolve_key(self, product: Dict[str, Any]) -> Key:
        key: Key = (product.get("shop_id"), product["item_id"])
        if key in self._items:
            return key
        existing = self._by_item.get(product["item_id"])
        if existing is not None and (existing[0] is None or key[0] is None):
            return existing
        return key

    def add(self, products: List[Dict[str, Any]], keyword: str) -> Tuple[int, int]:
        """Add one page of products. Returns (new_overall, new_for_this_keyword)."""
        seen_for_kw = self._kw_seen.setdefault(keyword, set())
        new_overall = new_for_keyword = 0
        for product in products:
            if product.get("item_id") is None:
                continue
            self.raw_count += 1
            key = self._resolve_key(product)

            if key not in seen_for_kw:
                seen_for_kw.add(key)
                new_for_keyword += 1

            existing = self._items.get(key)
            if existing is None:
                record = dict(product)
                record["keywords"] = keyword
                self._items[key] = record
                self._by_item.setdefault(product["item_id"], key)
                new_overall += 1
            else:
                for field in self.FILL_FIELDS:
                    if existing.get(field) is None and product.get(field) is not None:
                        existing[field] = product[field]
                kws = existing["keywords"].split(KEYWORD_SEP)
                if keyword not in kws:
                    kws.append(keyword)
                    existing["keywords"] = KEYWORD_SEP.join(kws)
        return new_overall, new_for_keyword

    def all(self) -> List[Dict[str, Any]]:
        return list(self._items.values())

    def keyword_counts(self) -> Dict[str, int]:
        return {kw: len(keys) for kw, keys in self._kw_seen.items()}

    @property
    def duplicates_removed(self) -> int:
        return self.raw_count - len(self._items)


# --------------------------------------------------------------------------- #
# CSV output
# --------------------------------------------------------------------------- #
def save_to_csv(products: List[Dict[str, Any]], filename: str = DEFAULT_OUTPUT_CSV,
                quiet: bool = False) -> int:
    """
    Save the products list to a CSV with csv.DictWriter.
    encoding="utf-8-sig" adds a BOM so Excel displays Thai/multi-language text correctly.
    The header is the union of dictionary keys (in order of first appearance).
    Returns the number of rows written (0 if nothing was saved).
    """
    if not products:
        if not quiet:
            print("[warn] No products to save; CSV file was not created.", file=sys.stderr)
        return 0

    fieldnames: List[str] = []
    for product in products:
        for key in product:
            if key not in fieldnames:
                fieldnames.append(key)

    tmp_name = f"{filename}.tmp"
    try:
        # Write to a temp file and swap it in, so a job that is stopped mid-write can never leave a
        # half-written CSV behind (the previous checkpoint stays intact).
        with open(tmp_name, "w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(products)
        os.replace(tmp_name, filename)
    except PermissionError:
        print(f"[error] Cannot write '{filename}'. Is it open in Excel? Close it and run again.",
              file=sys.stderr)
        return 0
    except OSError as exc:
        print(f"[error] Could not write '{filename}': {exc!r}", file=sys.stderr)
        try:
            os.remove(tmp_name)
        except OSError:
            pass
        return 0
    return len(products)


# --------------------------------------------------------------------------- #
# Diagnostics
# --------------------------------------------------------------------------- #
def describe(value: Any) -> str:
    if isinstance(value, dict):
        return f"dict[{len(value)}]"
    if isinstance(value, list):
        return f"list[{len(value)}]"
    return f"{type(value).__name__}={str(value)[:60]!r}"


def summarize_payload(index: int, payload: Any) -> None:
    print(f"[debug] Payload #{index}: type={type(payload).__name__}")
    if isinstance(payload, dict):
        for key, value in list(payload.items())[:15]:
            print(f"[debug]   {key!r}: {describe(value)}")


def dump_payloads(payloads: List[Any], prefix: str) -> None:
    Path(DEBUG_DIR).mkdir(exist_ok=True)
    for i, payload in enumerate(payloads, 1):
        path = Path(DEBUG_DIR) / f"{prefix}_{i}.json"
        try:
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[debug] Saved raw payload -> {path}")
        except Exception as exc:
            print(f"[warn] Could not save {path}: {exc!r}", file=sys.stderr)


# --------------------------------------------------------------------------- #
# DOM fallback (only used when the API yields nothing parseable)
# --------------------------------------------------------------------------- #
LINK_ID_RE = re.compile(r"-i\.(\d+)\.(\d+)")
PRICE_TEXT_RE = re.compile(r"฿\s*([\d,]+(?:\.\d+)?)")
SOLD_TEXT_RE = re.compile(r"ขายแล้ว\s*([\d.,]+\s*(?:พัน|หมื่น|แสน|ล้าน|k|K)?)")


def parse_dom_products(page: Page) -> List[Dict[str, Any]]:
    try:
        anchors = page.eval_on_selector_all(
            'a[href*="-i."]',
            "els => els.map(e => ({href: e.getAttribute('href'), text: e.innerText}))",
        )
    except Exception as exc:
        print(f"[warn] DOM fallback failed: {exc!r}", file=sys.stderr)
        return []

    products = []
    for a in anchors:
        match = LINK_ID_RE.search(a.get("href") or "")
        if not match:
            continue
        text = a.get("text") or ""
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        name = next((ln for ln in lines if len(ln) > 8 and not ln.startswith("฿")), None)
        price_m, sold_m = PRICE_TEXT_RE.search(text), SOLD_TEXT_RE.search(text)
        products.append({
            "item_id": int(match.group(2)),
            "shop_id": int(match.group(1)),
            "name": name,
            "price": normalize_price(price_m.group(1)) if price_m else None,
            "stock": None,  # not shown on search cards
            "historical_sold": to_int(sold_m.group(1)) if sold_m else None,
        })
    return products


# --------------------------------------------------------------------------- #
# Browser helpers
# --------------------------------------------------------------------------- #
def make_response_handler(captured: List[Any]):
    """Listener that stores the JSON of every search_items response."""

    def handle_response(response: Response) -> None:
        try:
            if API_MARKER not in response.url:
                return
            if response.status != 200:
                print(f"[warn] Search API returned HTTP {response.status}", file=sys.stderr)
                return
            body = response.text()
            if not body:
                return
            payload = json.loads(body)
            captured.append(payload)
            err = block_error(payload)
            note = f" (error={err!r})" if err is not None else ""
            print(f"    [api] Captured search_items response #{len(captured)}{note}")
        except json.JSONDecodeError:
            print("[warn] Search API response was not valid JSON", file=sys.stderr)
        except Exception as exc:
            print(f"[warn] Could not read response body: {exc!r}", file=sys.stderr)

    return handle_response


def human_scroll(page: Page, rounds: int = 8) -> None:
    """Scroll in small, jittery steps to trigger lazy-loaded product requests."""
    for i in range(rounds):
        page.mouse.move(random.randint(200, 1100), random.randint(200, 700))
        for _ in range(random.randint(3, 5)):
            page.mouse.wheel(0, random.randint(350, 650))
            page.wait_for_timeout(random.randint(250, 600))
        page.wait_for_timeout(random.randint(900, 1800))
        print(f"    [scroll] round {i + 1}/{rounds}")
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2500)


def needs_human_check(page: Page) -> bool:
    url = page.url.lower()
    return any(t in url for t in ("login", "verify", "captcha", "anti_fraud"))


def polite_pause(page: Page, bounds: Tuple[float, float], label: str) -> None:
    seconds = random.uniform(*bounds)
    print(f"[info] Waiting {seconds:.1f}s before {label}...")
    page.wait_for_timeout(int(seconds * 1000))


# --------------------------------------------------------------------------- #
# Environment detection and browser launching (local desktop vs. cloud server)
# --------------------------------------------------------------------------- #
INTERACTIVE = True  # set in main(); False means "nobody can answer prompts" (headless/cloud/no terminal)


def ask(message: str) -> str:
    """
    Prompt the user; returns '' for Enter, or the lowercase answer.
    In non-interactive runs (headless/cloud, or no terminal) nobody can answer, so it waits a few
    seconds and returns '' (= "retry/continue") instead of blocking forever.
    """
    if not INTERACTIVE:
        first_line = next((ln.strip() for ln in message.splitlines() if ln.strip()), "")
        wait = random.uniform(5, 10)
        print(f"[info] Non-interactive run, no one to ask ({first_line}). Continuing in {wait:.0f}s.")
        time.sleep(wait)
        return ""
    try:
        return input(message).strip().lower()
    except EOFError:
        return "s"


def env_flag(name: str) -> Optional[bool]:
    """Read a boolean environment variable; None if unset/empty."""
    value = os.environ.get(name)
    if value is None or not value.strip():
        return None
    return value.strip().lower() in ("1", "true", "yes", "on")


def has_display() -> bool:
    """Windows/macOS always have a GUI session; Linux needs DISPLAY or WAYLAND_DISPLAY."""
    if not sys.platform.startswith("linux"):
        return True
    return bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))


def is_cloud_environment() -> bool:
    """Best-effort detection of Streamlit Community Cloud / containers. Override: SHOPEE_CLOUD=1|0."""
    forced = env_flag("SHOPEE_CLOUD")
    if forced is not None:
        return forced
    markers = (
        Path("/mount/src").exists(),                        # Streamlit Community Cloud checkout path
        os.environ.get("HOME", "") == "/home/adminuser",     # Streamlit Community Cloud user
        Path("/.dockerenv").exists(),                        # generic Docker container
        bool(os.environ.get("KUBERNETES_SERVICE_HOST")),     # Kubernetes
    )
    return any(markers)


@dataclass
class BrowserConfig:
    profile_dir: str = DEFAULT_PROFILE_DIR
    channel: Optional[str] = None
    headless: bool = False
    ephemeral: bool = False              # True = standard browser + fresh context (no profile folder)
    server_mode: bool = False            # True = add container-safe Chromium flags
    storage_state: Optional[str] = None  # cookies/session JSON (input for scraping, output for --login)


class Session:
    """A launched browser: a persistent context, or a standard browser + context."""

    def __init__(self, context: BrowserContext, browser: Any = None, mode: str = "persistent") -> None:
        self.context = context
        self.browser = browser
        self.mode = mode

    def close(self) -> None:
        for obj in (self.context, self.browser):
            if obj is None:
                continue
            try:
                obj.close()
            except Exception:
                pass


# ----- error classification ------------------------------------------------- #
def error_text(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}".lower()


def is_target_closed_error(exc: BaseException) -> bool:
    text = error_text(exc)
    return type(exc).__name__ == "TargetClosedError" or "has been closed" in text or "target closed" in text


def is_missing_channel_error(exc: BaseException) -> bool:
    text = error_text(exc)
    return "distribution" in text and "not found" in text


def is_missing_browser_error(exc: BaseException) -> bool:
    text = error_text(exc)
    if is_missing_channel_error(exc):
        return False
    return "executable doesn't exist" in text or "playwright install" in text


def is_missing_deps_error(exc: BaseException) -> bool:
    text = error_text(exc)
    return ("missing dependencies" in text or "install-deps" in text
            or "error while loading shared libraries" in text)


def is_profile_lock_error(exc: BaseException) -> bool:
    text = error_text(exc)
    markers = ("processsingleton", "singletonlock", "profile appears to be in use",
               "opening in existing browser session", "already in use")
    return is_target_closed_error(exc) or any(m in text for m in markers)


# ----- recovery helpers ----------------------------------------------------- #
_INSTALL_ATTEMPTED = False


def install_chromium() -> bool:
    """One-time `playwright install chromium` (the browser is not shipped with the pip package)."""
    global _INSTALL_ATTEMPTED
    if _INSTALL_ATTEMPTED:
        return False
    _INSTALL_ATTEMPTED = True
    print("[info] Playwright's Chromium is missing; installing it now (one-time, can take a minute)...")
    try:
        result = subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"],
                                capture_output=True, text=True, timeout=900)
    except Exception as exc:
        print(f"[warn] Could not run 'playwright install chromium': {exc!r}", file=sys.stderr)
        return False
    if result.returncode != 0:
        tail = (result.stderr or result.stdout or "").strip().splitlines()[-3:]
        print("[warn] 'playwright install chromium' failed: " + " | ".join(tail), file=sys.stderr)
        return False
    print("[info] Chromium installed.")
    return True


def clear_stale_profile_locks(profile_dir: str) -> bool:
    """
    Remove Chromium's Singleton* lock files left behind by a crashed browser (Linux/macOS).
    Refuses to touch them if the PID recorded in SingletonLock is still running.
    Returns True only if stale locks were removed.
    """
    if os.name != "posix":
        return False
    base = Path(profile_dir)
    lock = base / "SingletonLock"
    if not (lock.is_symlink() or lock.exists()):
        return False
    try:
        pid = int(os.readlink(lock).rsplit("-", 1)[1])   # symlink target looks like "hostname-12345"
    except (OSError, ValueError, IndexError):
        return False                                      # cannot prove it is stale -> leave it alone
    if pid != os.getpid():
        try:
            os.kill(pid, 0)                               # signal 0 = "is this process alive?"
            print(f"[warn] Profile is in use by running process {pid}; not removing its lock.",
                  file=sys.stderr)
            return False
        except ProcessLookupError:
            pass                                          # process is gone -> lock is stale
        except PermissionError:
            return False                                  # alive, owned by someone else
    removed = False
    for name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        path = base / name
        try:
            if path.is_symlink() or path.exists():
                path.unlink()
                removed = True
        except OSError:
            pass
    if removed:
        print(f"[info] Removed stale browser profile locks in {base}.")
    return removed


def load_storage_state(path: Optional[str]) -> Optional[Dict[str, Any]]:
    """Saved login session: a JSON file (--storage-state / SHOPEE_STORAGE_STATE) or raw JSON in
    SHOPEE_STORAGE_STATE_JSON (handy for Streamlit secrets). Returns None if nothing usable."""
    path = path or os.environ.get("SHOPEE_STORAGE_STATE")
    raw_json = os.environ.get("SHOPEE_STORAGE_STATE_JSON")
    try:
        if path and Path(path).exists():
            return json.loads(Path(path).read_text(encoding="utf-8"))
        if raw_json:
            return json.loads(raw_json)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"[warn] Could not load the saved session: {exc!r}", file=sys.stderr)
    return None


# ----- launching ------------------------------------------------------------ #
def launch_args(cfg: BrowserConfig) -> List[str]:
    return LAUNCH_ARGS + (SERVER_LAUNCH_ARGS if cfg.server_mode else [])


def context_options() -> Dict[str, Any]:
    return dict(
        user_agent=USER_AGENT,
        viewport=VIEWPORT,
        locale=LOCALE,
        timezone_id=TIMEZONE,
        extra_http_headers={"Accept-Language": "th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7"},
    )


def _launch_persistent(pw, cfg: BrowserConfig, channel: Optional[str]) -> Session:
    Path(cfg.profile_dir).mkdir(parents=True, exist_ok=True)
    kwargs: Dict[str, Any] = dict(
        user_data_dir=str(Path(cfg.profile_dir).resolve()),
        headless=cfg.headless,
        args=launch_args(cfg),
        ignore_default_args=["--enable-automation"],
        **context_options(),
    )
    if channel:
        kwargs["channel"] = channel
    return Session(pw.chromium.launch_persistent_context(**kwargs), mode="persistent")


def _launch_ephemeral(pw, cfg: BrowserConfig, channel: Optional[str]) -> Session:
    kwargs: Dict[str, Any] = dict(
        headless=cfg.headless,
        args=launch_args(cfg),
        ignore_default_args=["--enable-automation"],
    )
    if channel:
        kwargs["channel"] = channel
    browser = pw.chromium.launch(**kwargs)
    try:
        options = context_options()
        state = load_storage_state(cfg.storage_state)
        if state:
            options["storage_state"] = state
            print("[info] Loaded saved Shopee session (cookies) into the browser context.")
        context = browser.new_context(**options)
    except Exception:
        try:
            browser.close()
        except Exception:
            pass
        raise
    return Session(context, browser, mode="ephemeral")


def _with_recovery(launch, profile_dir: Optional[str] = None) -> Session:
    """Run `launch()`; on a known failure try one fix (install browser / clear stale lock) and retry."""
    try:
        return launch()
    except Exception as exc:
        recovered = False
        if is_missing_browser_error(exc):
            recovered = install_chromium()
        elif profile_dir and is_profile_lock_error(exc):
            recovered = clear_stale_profile_locks(profile_dir)
        if not recovered:
            raise
        return launch()


def open_session(pw, cfg: BrowserConfig) -> Session:
    """
    Launch the browser safely for the current environment:
      1. persistent profile (unless cfg.ephemeral), recovering from a missing browser binary or
         stale profile locks once;
      2. otherwise / on failure, a standard browser + fresh context (no profile, no locks);
      3. a requested channel (e.g. "chrome") that isn't installed falls back to bundled Chromium.
    """
    channels: List[Optional[str]] = [cfg.channel, None] if cfg.channel else [None]
    last_exc: Optional[Exception] = None

    if not cfg.ephemeral:
        for channel in channels:
            try:
                return _with_recovery(lambda: _launch_persistent(pw, cfg, channel), cfg.profile_dir)
            except Exception as exc:
                last_exc = exc
                if channel and is_missing_channel_error(exc):
                    print(f"[warn] Browser channel '{channel}' not found; using bundled Chromium.",
                          file=sys.stderr)
                    continue
                reason = (str(exc).strip().splitlines() or [type(exc).__name__])[0]
                print(f"[warn] Persistent profile launch failed ({type(exc).__name__}: {reason}).",
                      file=sys.stderr)
                break
        print("[warn] Falling back to a standard (non-persistent) browser context.", file=sys.stderr)

    for channel in channels:
        try:
            return _with_recovery(lambda: _launch_ephemeral(pw, cfg, channel))
        except Exception as exc:
            last_exc = exc
            if channel and is_missing_channel_error(exc):
                print(f"[warn] Browser channel '{channel}' not found; using bundled Chromium.",
                      file=sys.stderr)
                continue
            break

    raise last_exc if last_exc else RuntimeError("Could not launch the browser")


def print_launch_help(exc: BaseException) -> None:
    lines = str(exc).strip().splitlines()
    headline = lines[0] if lines else ""
    print(f"[error] Could not launch the browser: {type(exc).__name__}: {headline}", file=sys.stderr)
    if is_missing_deps_error(exc):
        print("        Chromium's system libraries are missing. On Streamlit Community Cloud list them in\n"
              "        packages.txt (libnss3, libnspr4, libatk1.0-0, libatk-bridge2.0-0, libcups2, libdrm2,\n"
              "        libgbm1, libxkbcommon0, libasound2, ...) and reboot the app.", file=sys.stderr)
    elif is_missing_browser_error(exc):
        print("        The Playwright browser is not installed. Run: python -m playwright install chromium",
              file=sys.stderr)
    elif is_profile_lock_error(exc):
        print("        The browser exited right after starting. Common causes: low memory, a tiny /dev/shm,\n"
              "        missing system libraries, or (locally) the profile folder open in another browser.",
              file=sys.stderr)


def build_search_url(keyword: str, page_idx: int) -> str:
    """Shopee pages are zero-indexed: page=0 is the first page of results."""
    return f"{BASE_URL}/search?keyword={quote(keyword)}&page={page_idx}"


# --------------------------------------------------------------------------- #
# One page of one keyword
# --------------------------------------------------------------------------- #
def scrape_page(page: Page, captured: List[Any], keyword: str, page_idx: int,
                scroll_rounds: int, retries: int, debug: bool) -> Tuple[List[Dict[str, Any]], str]:
    """
    Load one search results page and return (products, status).

    status:
      "ok"      products were extracted
      "end"     Shopee answered normally but with no items (past the last page)
      "blocked" blocked/no usable response after all retries
      "skip"    the user chose to skip the rest of this keyword
      "quit"    the user chose to stop the whole run
    """
    url = build_search_url(keyword, page_idx)
    dump_prefix = f"{slugify(keyword)}_p{page_idx + 1}"
    dom_products: List[Dict[str, Any]] = []

    for attempt in range(1, retries + 1):
        if attempt > 1:
            print(f"[info] Retry {attempt}/{retries} for '{keyword}' page {page_idx + 1}")
        captured.clear()

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        except PlaywrightTimeoutError:
            print("[warn] Navigation timed out; continuing.", file=sys.stderr)

        if needs_human_check(page):
            answer = ask(
                "\n[action] Shopee wants you to log in / verify. Do it in the browser window\n"
                "         (the session is saved to the profile), then press Enter to continue\n"
                "         ('s' = skip this keyword, 'q' = quit and save): "
            )
            if answer == "q":
                return [], "quit"
            if answer == "s":
                return [], "skip"
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            except PlaywrightTimeoutError:
                print("[warn] Navigation timed out; continuing.", file=sys.stderr)

        try:
            page.wait_for_load_state("networkidle", timeout=20_000)
        except PlaywrightTimeoutError:
            print("[warn] Network never went idle; continuing.", file=sys.stderr)

        human_scroll(page, rounds=scroll_rounds)
        if not captured:
            page.wait_for_timeout(5000)

        products = dedupe([p for payload in list(captured) for p in parse_payload(payload)])
        if products:
            if debug:
                for i, payload in enumerate(captured, 1):
                    summarize_payload(i, payload)
                dump_payloads(captured, dump_prefix)
            return products, "ok"

        errors = [e for e in (block_error(p) for p in captured) if e is not None]

        # Normal response, zero items: past the last page (or a parser/schema problem on page 1).
        if captured and not errors:
            if page_idx == 0:
                print("[warn] API responded but 0 products parsed on the first page; "
                      "the schema may have changed. Saving payloads for inspection.", file=sys.stderr)
                for i, payload in enumerate(captured, 1):
                    summarize_payload(i, payload)
                dump_payloads(captured, dump_prefix)
                dom_products = parse_dom_products(page)
                if dom_products:
                    print(f"[info] Using DOM fallback ({len(dom_products)} cards).")
                    return dedupe(dom_products), "ok"
            print(f"[info] No more results for '{keyword}' after page {page_idx}.")
            return [], "end"

        # Blocked, or nothing captured at all.
        dom_products = parse_dom_products(page)
        if debug or attempt == retries:
            for i, payload in enumerate(captured, 1):
                summarize_payload(i, payload)
            if captured:
                dump_payloads(captured, dump_prefix)

        if attempt < retries:
            reason = f"API error {errors[0]!r}" if errors else "no search_items response captured"
            answer = ask(
                f"\n[action] {reason}. In the browser, log in / solve any captcha, then press\n"
                "         Enter to retry ('s' = skip this keyword, 'q' = quit and save): "
            )
            if answer == "q":
                return [], "quit"
            if answer == "s":
                return [], "skip"

    if dom_products:
        print(f"[info] API unavailable; using DOM fallback ({len(dom_products)} cards).")
        return dedupe(dom_products), "ok"
    return [], "blocked"


# --------------------------------------------------------------------------- #
# All keywords x all pages
# --------------------------------------------------------------------------- #
MAX_BROWSER_RESTARTS = 2  # headless only: relaunch if the browser crashes mid-run (e.g. out of memory)


def scrape_all(keywords: List[str], max_pages: int, cfg: BrowserConfig, scroll_rounds: int,
               retries: int, debug: bool, output_csv: str) -> ProductStore:
    store = ProductStore()
    captured: List[Any] = []
    total_keywords = len(keywords)

    with sync_playwright() as pw:
        def start_browser() -> Tuple[Session, Page]:
            session = open_session(pw, cfg)
            session.context.add_init_script(STEALTH_JS)
            session.context.on("response", make_response_handler(captured))  # covers every page/tab
            page = session.context.pages[0] if session.context.pages else session.context.new_page()
            return session, page

        try:
            session, page = start_browser()
        except Exception as exc:
            print_launch_help(exc)
            return store
        print(f"[info] Browser ready ({session.mode} context, headless={cfg.headless}).")

        restarts = 0
        try:
            for k_idx, keyword in enumerate(keywords, 1):
                print(f"\n{'=' * 70}\n[keyword {k_idx}/{total_keywords}] '{keyword}' "
                      f"(up to {max_pages} pages)\n{'=' * 70}")
                stop_all = False

                for page_idx in range(max_pages):
                    print(f"[keyword {k_idx}/{total_keywords}] '{keyword}' - "
                          f"page {page_idx + 1}/{max_pages}  ->  {build_search_url(keyword, page_idx)}")

                    while True:
                        try:
                            products, status = scrape_page(
                                page, captured, keyword, page_idx, scroll_rounds, retries, debug)
                            break
                        except Exception as exc:
                            crashed = cfg.headless and is_target_closed_error(exc)
                            if not crashed or restarts >= MAX_BROWSER_RESTARTS:
                                raise
                            restarts += 1
                            print(f"[warn] Browser closed unexpectedly ({type(exc).__name__}); restarting it "
                                  f"({restarts}/{MAX_BROWSER_RESTARTS})...", file=sys.stderr)
                            session.close()
                            session, page = start_browser()

                    if status == "quit":
                        stop_all = True
                        break
                    if status != "ok":
                        reason = {"end": "end of results", "skip": "skipped by user",
                                  "blocked": "blocked / no data"}.get(status, status)
                        print(f"[info] Stopping '{keyword}': {reason}.")
                        break

                    new_overall, new_for_kw = store.add(products, keyword)
                    print(f"[page {page_idx + 1}] {len(products)} products parsed | "
                          f"{new_for_kw} new for this keyword | {new_overall} new overall | "
                          f"{len(store)} unique total")

                    if new_for_kw == 0:
                        print(f"[info] No new products for '{keyword}' (pagination not advancing); "
                              "stopping this keyword.")
                        break
                    if page_idx < max_pages - 1:
                        polite_pause(page, PAGE_DELAY, "the next page")

                print(f"[keyword {k_idx}/{total_keywords}] '{keyword}' finished: "
                      f"{store.keyword_counts().get(keyword, 0)} unique products "
                      f"({len(store)} unique overall)")

                # Checkpoint so nothing is lost if a later keyword crashes or is interrupted.
                if len(store):
                    save_to_csv(store.all(), output_csv, quiet=True)

                if stop_all:
                    print("[info] Quit requested; stopping early.")
                    break
                if k_idx < total_keywords:
                    polite_pause(page, KEYWORD_DELAY, "the next keyword")
        except KeyboardInterrupt:
            print("\n[info] Interrupted by user; saving what has been collected so far.")
        except Exception as exc:
            print(f"[error] Unexpected browser error: {type(exc).__name__}: {exc}", file=sys.stderr)
        finally:
            session.close()  # flushes cookies/session into the profile folder (persistent mode)

    return store


# --------------------------------------------------------------------------- #
# Manual login helper (needs a visible browser, so it only works on your own computer)
# --------------------------------------------------------------------------- #
def login_session(cfg: BrowserConfig) -> None:
    """
    Open Shopee in the persistent profile so you can log in / solve a captcha by hand. The session
    is saved to the profile folder. If --storage-state FILE was given, it is also exported there
    so a headless server (Streamlit Cloud) can reuse it. Close the browser window when done.
    """
    if cfg.headless:
        print("[error] Manual login needs a visible browser window. Run this on your own computer "
              "(use --headed), not on a headless server.", file=sys.stderr)
        return

    export_path = cfg.storage_state
    with sync_playwright() as pw:
        try:
            session = open_session(pw, dc_replace(cfg, ephemeral=False))
        except Exception as exc:
            print_launch_help(exc)
            return

        context = session.context
        context.add_init_script(STEALTH_JS)
        page = context.pages[0] if context.pages else context.new_page()
        try:
            page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60_000)
        except PlaywrightTimeoutError:
            print("[warn] Navigation timed out; continuing.", file=sys.stderr)
        print("[info] Log in / solve any captcha, then CLOSE the browser window to save the session.")

        last_state: Optional[Dict[str, Any]] = None
        try:
            if export_path:
                # Snapshot the session while the window is open; it is written once you close it.
                while True:
                    try:
                        if not context.pages:
                            break
                        last_state = context.storage_state()
                    except Exception:
                        break
                    time.sleep(1.5)
            else:
                context.wait_for_event("close", timeout=0)  # returns when the window is closed
        except KeyboardInterrupt:
            pass
        except Exception:
            pass
        finally:
            if export_path and last_state:
                try:
                    Path(export_path).write_text(
                        json.dumps(last_state, ensure_ascii=False, indent=2), encoding="utf-8")
                    print(f"[info] Saved session to '{export_path}'. Keep this file private "
                          "(it contains your login cookies) and never commit it to a public repo.")
                except OSError as exc:
                    print(f"[warn] Could not write '{export_path}': {exc!r}", file=sys.stderr)
            session.close()


# --------------------------------------------------------------------------- #
# Graceful shutdown (the dashboard stops a job that runs too long or hangs)
# --------------------------------------------------------------------------- #
def install_graceful_shutdown() -> None:
    """
    Turn SIGTERM (and Ctrl-Break on Windows) into KeyboardInterrupt. scrape_all() already handles that
    by closing the browser and returning what it has collected, and main() then saves the CSV, so a
    job stopped for taking too long still keeps its partial results.
    """
    def handler(signum, frame):
        raise KeyboardInterrupt

    for name in ("SIGTERM", "SIGBREAK"):
        sig = getattr(signal, name, None)
        if sig is not None:
            try:
                signal.signal(sig, handler)
            except (ValueError, OSError):   # not on the main thread / unsupported here
                pass


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def main() -> None:
    global INTERACTIVE
    install_graceful_shutdown()

    parser = argparse.ArgumentParser(
        description="Scrape Shopee Thailand search results for multiple keywords with pagination.")
    parser.add_argument("--keywords", nargs="+", default=DEFAULT_KEYWORDS,
                        help=f"Keywords to scrape (default: {' '.join(DEFAULT_KEYWORDS)})")
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES,
                        help="Maximum result pages per keyword")
    parser.add_argument("--scrolls", type=int, default=8, help="Scroll rounds per page")
    parser.add_argument("--retries", type=int, default=2,
                        help="Attempts per page (with a manual-fix pause between attempts)")
    parser.add_argument("--profile", default=DEFAULT_PROFILE_DIR, help="User data dir (persistent mode)")
    parser.add_argument("--channel", default=None, help='Browser channel, e.g. "chrome"')
    parser.add_argument("--output", default=DEFAULT_OUTPUT_CSV, help="Combined CSV output file")

    display = parser.add_mutually_exclusive_group()
    display.add_argument("--headless", action="store_true",
                         help="Force a headless browser (default on servers/cloud)")
    display.add_argument("--headed", action="store_true",
                         help="Force a visible browser window (default on your own computer)")
    storage = parser.add_mutually_exclusive_group()
    storage.add_argument("--ephemeral", action="store_true",
                         help="Use a standard browser + fresh context instead of the profile folder "
                              "(default on servers/cloud)")
    storage.add_argument("--persistent", action="store_true",
                         help="Force the persistent profile folder (default on your own computer)")
    parser.add_argument("--storage-state", default=None, metavar="FILE",
                        help="Saved login session JSON: loaded when scraping in ephemeral mode, "
                             "written when used with --login")

    parser.add_argument("--limit-keywords", type=int, default=None, metavar="N",
                        help="Hard cap: refuse to run if more than N keywords are given (plan limit)")
    parser.add_argument("--limit-pages", type=int, default=None, metavar="N",
                        help="Hard cap: refuse to run if --max-pages is greater than N (plan limit)")
    parser.add_argument("--login", action="store_true",
                        help="Only open Shopee in the profile for manual login/captcha, then exit")
    parser.add_argument("--no-print-json", action="store_true",
                        help="Do not print the full product list as JSON")
    parser.add_argument("--debug", action="store_true",
                        help=f"Print payload structure and save raw JSON to ./{DEBUG_DIR}/")
    args = parser.parse_args()

    # ---- decide how to run: desktop vs. cloud server --------------------------------------
    cloud = is_cloud_environment()
    display_available = has_display()

    if args.headless:
        headless = True
    elif args.headed:
        headless = False
    else:
        env_headless = env_flag("SHOPEE_HEADLESS")
        headless = env_headless if env_headless is not None else (cloud or not display_available)

    if args.ephemeral:
        ephemeral = True
    elif args.persistent:
        ephemeral = False
    else:
        env_ephemeral = env_flag("SHOPEE_EPHEMERAL")
        ephemeral = env_ephemeral if env_ephemeral is not None else cloud

    server_mode = cloud or (sys.platform.startswith("linux") and not display_available)

    try:
        has_terminal = sys.stdin is not None and sys.stdin.isatty()
    except (ValueError, OSError):
        has_terminal = False
    INTERACTIVE = (not headless) and has_terminal

    cfg = BrowserConfig(profile_dir=args.profile, channel=args.channel, headless=headless,
                        ephemeral=ephemeral, server_mode=server_mode, storage_state=args.storage_state)
    print(f"[info] Environment: cloud={cloud}, display={display_available} | headless={headless}, "
          f"context={'standard (no profile)' if ephemeral else 'persistent profile'}, "
          f"container-safe flags={server_mode}, interactive={INTERACTIVE}")

    if args.login:
        login_session(cfg)
        return

    keywords = list(dict.fromkeys(k.strip() for k in args.keywords if k.strip()))
    if not keywords:
        parser.error("Provide at least one non-empty keyword.")
    # Plan limits are passed in by the dashboard; refuse (never silently trim) when exceeded.
    if args.limit_keywords is not None and len(keywords) > args.limit_keywords:
        parser.error(f"{len(keywords)} keywords requested but the plan allows {args.limit_keywords} per run.")
    if args.limit_pages is not None and args.max_pages > args.limit_pages:
        parser.error(f"{args.max_pages} pages per keyword requested but the plan allows {args.limit_pages}.")
    max_pages = max(1, args.max_pages)

    print(f"[info] Keywords: {keywords}")
    print(f"[info] Max pages per keyword: {max_pages} | output: {args.output}")

    store = scrape_all(keywords, max_pages, cfg, args.scrolls, max(1, args.retries),
                       args.debug, args.output)
    products = store.all()

    if not args.no_print_json:
        print(json.dumps(products, indent=2, ensure_ascii=False))

    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    for keyword in keywords:
        print(f"  {keyword!r}: {store.keyword_counts().get(keyword, 0)} unique products")
    print(f"  Duplicates removed: {store.duplicates_removed}")
    print(f"Total unique products: {len(products)}")

    saved = save_to_csv(products, args.output)
    if saved:
        print(f"[success] Saved {saved} products to '{args.output}'")


if __name__ == "__main__":
    main()