#!/usr/bin/env python3
"""
Streamlit dashboard for the Shopee Thailand scraper.

Run it from the folder that contains this file and shopee_scraper_multi.py:

    pip install -U streamlit pandas playwright
    playwright install chromium
    streamlit run app.py

How it works: the dashboard starts `shopee_scraper_multi.py` as a separate process (this avoids
Playwright/asyncio conflicts inside Streamlit), streams its log to show live progress, then loads
the CSV it produced into an interactive table with filters and a download button.

Accounts live in Supabase Auth; the plan (Free/Pro) is read from the `users` table, which only the
Stripe webhook service (webhook_server.py) can change. Secrets needed (env vars or Streamlit secrets):
SUPABASE_URL, SUPABASE_PUBLISHABLE_KEY (or SUPABASE_ANON_KEY), STRIPE_PAYMENT_LINK, STRIPE_PORTAL_LINK.

On your own computer the scraper opens a real browser window; on a server such as Streamlit Cloud
it automatically runs headless with container-safe flags (see shopee_scraper_multi.py).
"""

import logging
import os
import re
import subprocess
import sys
import time
from collections import deque
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urlencode

import pandas as pd
import streamlit as st

import plan_limits as pl
import scraper_runner as runner
import supabase_backend as backend

try:  # same detection the scraper uses, so the UI matches how the scraper will run
    from shopee_scraper_multi import has_display, is_cloud_environment
    SERVER_MODE = is_cloud_environment() or not has_display()
except Exception:
    SERVER_MODE = False

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
BASE_DIR = Path(__file__).resolve().parent
SCRAPER = BASE_DIR / "shopee_scraper_multi.py"
OUTPUT_DIR = BASE_DIR / "dashboard_output"
DEFAULT_PROFILE = "./shopee_profile"          # resolved relative to BASE_DIR
SHOPEE_BASE = "https://shopee.co.th"
PRESET_KEYWORDS = ["skincare", "makeup", "sunscreen", "serum", "moisturizer", "lipstick"]
DEFAULT_SELECTION = ["skincare"]
KEYWORD_SEP = "|"                             # separator used by the scraper's `keywords` column
LOG_LINES_SHOWN = 12
OUTPUT_KEEP_PER_USER = 5                  # newest CSVs kept per user on disk
OUTPUT_MAX_AGE_SECONDS = 24 * 3600        # ...and nothing older than a day
log = logging.getLogger("dashboard")

# Everything held in st.session_state for a signed-in user; wiped on logout.
SESSION_KEYS_TO_CLEAR = (
    "authenticated", "sb", "user_id", "email", "username", "plan", "stripe_customer_id",
    "profile_checked_at", "profile_error", "df", "csv_bytes", "csv_name", "log",
)
PROFILE_TTL_SECONDS = 30   # how often the plan is re-read from Supabase (picks up webhook upgrades)

# ---- Subscription / pricing (Stripe) ---------------------------------------------------------
# Replace the placeholders below (or set STRIPE_PAYMENT_LINK / STRIPE_PORTAL_LINK as env vars or
# Streamlit secrets). The plan copy is placeholder text: edit PLANS to match what you really sell.
DEFAULT_PAYMENT_LINK = "https://buy.stripe.com/test_aFafZj5046glexo1ZK6oo00"
DEFAULT_PORTAL_LINK = "https://billing.stripe.com/p/login/test_placeholder"
UPGRADE_LABEL = "Upgrade to Pro ($15/month)"
PLANS = {
    "free": {
        "name": "Free Tier",
        "price": "$0",
        "features": [
            f"Multi-keyword Shopee search ({pl.describe('free')})",
            "Interactive results table with filters",
            "CSV export",
        ],
    },
    "pro": {
        "name": "Pro Tier",
        "price": "$15",
        "features": [
            "Everything in Free",
            f"Higher limits: {pl.describe('pro')}",
            "Saved search history (coming soon)",
            "Priority support",
        ],
    },
}

# Progress is parsed from the scraper's log lines.
RE_PAGE = re.compile(r"\[keyword (\d+)/(\d+)\] '(.*)' - page (\d+)/(\d+)")
RE_DONE = re.compile(r"\[keyword (\d+)/(\d+)\] '(.*)' finished: (\d+) unique products")
RE_TOTAL = re.compile(r"(\d+) unique total")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def collect_keywords(selected: List[str], custom: str) -> List[str]:
    """Merge multiselect choices with comma/newline separated custom keywords."""
    items = list(selected) + re.split(r"[,\n]", custom or "")
    cleaned = [k.strip().lstrip("-").strip() for k in items]  # a leading '-' would look like a CLI flag
    return list(dict.fromkeys(k for k in cleaned if k))


def build_command(keywords: List[str], max_pages: int, scrolls: int, profile: str,
                  use_chrome: bool, output_csv: Path, plan: str = "free") -> List[str]:
    limits = pl.limits_for(plan)
    cmd = [
        sys.executable, "-u", str(SCRAPER),
        "--keywords", *keywords,
        "--max-pages", str(max_pages),
        "--scrolls", str(scrolls),
        "--retries", "2",
        "--profile", profile,
        "--output", str(output_csv),
        "--no-print-json",
        # The scraper re-checks these and refuses to run if they are exceeded.
        "--limit-keywords", str(limits.max_keywords_per_run),
        "--limit-pages", str(limits.max_pages_per_keyword),
    ]
    if use_chrome:
        cmd += ["--channel", "chrome"]
    return cmd


def max_concurrent_scrapes() -> int:
    """Server-wide cap on simultaneous scrapes (each one runs a full browser). Override with the
    MAX_CONCURRENT_SCRAPES setting. A desktop install shares ONE browser profile, so it allows one."""
    try:
        configured = int(get_setting("MAX_CONCURRENT_SCRAPES", "0"))
    except ValueError:
        configured = 0
    return configured if configured > 0 else (2 if SERVER_MODE else 1)


def format_duration(seconds: float) -> str:
    seconds = int(seconds)
    return f"{seconds // 60}m{seconds % 60:02d}s"


def run_scraper(keywords: List[str], max_pages: int, scrolls: int, profile: str,
                use_chrome: bool, output_csv: Path, plan: str = "free", *,
                max_runtime: Optional[float] = None,
                stall_timeout: Optional[float] = None) -> "runner.RunResult":
    """
    Run the scraper subprocess with a runtime limit and a hang (no-output) limit, streaming its
    progress into the page. Whatever happens, the process tree is cleaned up before this returns.
    Refuses to start if the job exceeds the plan's limits.
    """
    check = pl.validate_job(plan, keywords, max_pages)
    if not check.ok:
        raise pl.PlanLimitError(" ".join(check.errors))
    cmd = build_command(keywords, max_pages, scrolls, profile, use_chrome, output_csv, plan)

    default_runtime, default_stall = runner.compute_timeouts(len(keywords), max_pages)
    max_runtime = max_runtime or default_runtime
    stall_timeout = stall_timeout or default_stall

    progress = st.progress(0.0, text="Starting the browser...")
    log_box = st.empty()
    recent: "deque[str]" = deque(maxlen=LOG_LINES_SHOWN)
    ui = {"fraction": 0.0, "message": "Starting the browser...", "unique": 0}
    started = time.monotonic()

    def on_line(line: str) -> None:
        recent.append(line)
        if m := RE_PAGE.search(line):
            k, total_k, kw, page, total_p = m.groups()
            ui["fraction"] = ((int(k) - 1) + (int(page) - 1) / int(total_p)) / int(total_k)
            ui["message"] = f"Keyword {k}/{total_k} '{kw}' - page {page}/{total_p}"
        elif m := RE_DONE.search(line):
            k, total_k, kw, count = m.group(1), m.group(2), m.group(3), m.group(4)
            ui["fraction"] = int(k) / int(total_k)
            ui["message"] = f"Finished '{kw}' ({count} products)"
        elif "Waiting" in line and "before" in line:
            ui["message"] = "Pausing between requests (keeps the scraper polite)..."
        elif "[scroll]" in line:
            ui["message"] = ui["message"].split(" - scrolling")[0] + " - scrolling"
        if m := RE_TOTAL.search(line):
            ui["unique"] = int(m.group(1))

        suffix = f" | {ui['unique']} unique products" if ui["unique"] else ""
        elapsed = format_duration(time.monotonic() - started)
        limit = format_duration(max_runtime)
        progress.progress(min(max(ui["fraction"], 0.0), 1.0),
                          text=f"{ui['message']}{suffix} | {elapsed} of max {limit}")
        log_box.code("\n".join(recent), language=None)

    result = runner.run_subprocess(
        cmd, cwd=str(BASE_DIR), env=runner.scraper_env(),
        max_runtime=max_runtime, stall_timeout=stall_timeout, on_line=on_line)

    final_text = {
        "completed": "Scraping complete",
        "timeout": "Stopped: the run took too long",
        "stalled": "Stopped: the scraper stopped responding",
        "failed": "The scraper exited with an error",
    }.get(result.status, "Finished")
    progress.progress(1.0 if result.ok else min(max(ui["fraction"], 0.0), 1.0), text=final_text)
    log_box.code("\n".join(list(result.lines)[-LOG_LINES_SHOWN:]), language=None)
    return result


def load_results(csv_path: Path) -> Optional[pd.DataFrame]:
    """Read the scraper's CSV (UTF-8 with BOM) into a tidy DataFrame."""
    if not csv_path.exists():
        return None
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return None
    if df.empty:
        return None

    for col in ("item_id", "shop_id", "stock", "historical_sold"):
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    if "price" in df:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
    if {"shop_id", "item_id"} <= set(df.columns):
        df["url"] = [
            f"{SHOPEE_BASE}/product/{int(s)}/{int(i)}" if pd.notna(s) and pd.notna(i) else None
            for s, i in zip(df["shop_id"], df["item_id"])
        ]
    return df


def csv_bytes_for_excel(df: pd.DataFrame) -> bytes:
    """CSV bytes with a UTF-8 BOM so Excel shows Thai text correctly."""
    return ("\ufeff" + df.to_csv(index=False)).encode("utf-8")


def show_dataframe(df: pd.DataFrame, **kwargs) -> None:
    """st.dataframe stretched to full width, across old and new Streamlit versions."""
    try:
        st.dataframe(df, width="stretch", **kwargs)
    except Exception:
        st.dataframe(df, use_container_width=True, **kwargs)


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Render the filter widgets and return the filtered view."""
    view = df
    col_kw, col_search = st.columns([1, 1])

    if "keywords" in df:
        options = sorted({k for s in df["keywords"].dropna() for k in str(s).split(KEYWORD_SEP)})
        chosen = col_kw.multiselect("Filter by keyword", options, placeholder="All keywords")
        if chosen:
            view = view[view["keywords"].fillna("").apply(
                lambda s: any(k in str(s).split(KEYWORD_SEP) for k in chosen))]

    query = col_search.text_input("Search product name", placeholder="e.g. sunscreen SPF50")
    if query and "name" in view:
        view = view[view["name"].fillna("").str.contains(query, case=False, regex=False)]

    if "price" in df and df["price"].notna().any():
        lo, hi = float(df["price"].min()), float(df["price"].max())
        if lo < hi:
            price_range = st.slider("Price range (THB)", lo, hi, (lo, hi))
            if price_range != (lo, hi):
                view = view[view["price"].between(*price_range)]
    return view


# --------------------------------------------------------------------------- #
# Authentication (Supabase Auth) and plan lookup (Supabase `users` table)
# --------------------------------------------------------------------------- #
def get_setting(name: str, default: str = "") -> str:
    """Config value from an environment variable or Streamlit secrets, else `default`."""
    value = os.environ.get(name)
    if not value:
        try:
            value = st.secrets.get(name)
        except Exception:  # no secrets.toml configured
            value = None
    return str(value) if value else default


def supabase_settings() -> Tuple[str, str]:
    """(url, publishable key). The legacy anon key works too. Never put the secret key here."""
    key = get_setting("SUPABASE_PUBLISHABLE_KEY") or get_setting("SUPABASE_ANON_KEY")
    return get_setting("SUPABASE_URL"), key


def supabase_configured() -> bool:
    url, key = supabase_settings()
    return bool(url and key)


def get_client():
    """
    One Supabase client PER browser session. It holds that user's login token, so never share it
    across users (do not wrap it in st.cache_resource).
    """
    client = st.session_state.get("sb")
    if client is None:
        url, key = supabase_settings()
        client = backend.make_client(url, key)
        st.session_state["sb"] = client
    return client


def is_authenticated() -> bool:
    return bool(st.session_state.get("authenticated", False))


def ensure_session() -> bool:
    """True while the Supabase session is still valid (expired tokens are refreshed automatically)."""
    client = st.session_state.get("sb")
    return client is not None and backend.session_is_valid(client)


def rerun() -> None:
    """st.rerun() on current Streamlit, st.experimental_rerun() on older versions."""
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.experimental_rerun()


def start_session(result: "backend.AuthResult") -> None:
    """Remember who signed in, then load their plan from the database."""
    st.session_state.update(
        authenticated=True,
        user_id=result.user_id,
        email=result.email,
        username=result.username or (result.email.split("@")[0] if result.email else "user"),
    )
    refresh_profile(force=True)


def logout() -> None:
    """Sign out of Supabase and drop everything held for this session (results, CSV, plan, client)."""
    client = st.session_state.get("sb")
    if client is not None:
        backend.sign_out(client)
    for key in SESSION_KEYS_TO_CLEAR:
        st.session_state.pop(key, None)


def require_login() -> bool:
    """Gate for the whole dashboard. Returns True only for a valid signed-in session."""
    if is_authenticated():
        if ensure_session():
            return True
        logout()
        st.warning("Your session expired. Please log in again.")
    render_login()
    return False


def _render_login_form() -> None:
    with st.form("login_form"):
        email = st.text_input("Email", key="login_email")
        password = st.text_input("Password", type="password", key="login_password")
        submitted = st.form_submit_button("Log in", type="primary")
    if not submitted:
        return
    if not email.strip() or not password:
        st.error("Enter your email and password.")
        return
    try:
        result = backend.sign_in(get_client(), email, password)
    except Exception as exc:  # e.g. invalid Supabase URL/key
        st.error(backend.friendly_error(exc))
        return
    if result.ok:
        start_session(result)
        rerun()
    else:
        st.error(result.message)


def _render_signup_form() -> None:
    with st.form("signup_form"):
        username = st.text_input("Username", key="signup_username")
        email = st.text_input("Email", key="signup_email")
        password = st.text_input("Password (min. 8 characters)", type="password", key="signup_password")
        confirm = st.text_input("Confirm password", type="password", key="signup_confirm")
        submitted = st.form_submit_button("Create account", type="primary")
    if not submitted:
        return
    if not username.strip() or not email.strip():
        st.error("Username and email are required.")
    elif len(password) < 8:
        st.error("Use a password with at least 8 characters.")
    elif password != confirm:
        st.error("Passwords do not match.")
    else:
        try:
            result = backend.sign_up(get_client(), email, password, username)
        except Exception as exc:
            st.error(backend.friendly_error(exc))
            return
        if not result.ok:
            st.error(result.message)
        elif result.needs_confirmation:
            st.success(result.message)
        else:
            start_session(result)
            rerun()


def render_login() -> None:
    """Centered Log in / Sign up card shown instead of the dashboard while signed out."""
    st.markdown("<style>[data-testid='stSidebar'] {display: none;}</style>", unsafe_allow_html=True)
    _, middle, _ = st.columns([1, 1.4, 1])
    with middle:
        st.write("")
        with st.container(border=True):
            st.title("🛍️ Shopee Scraper")
            if not supabase_configured():
                st.error("Supabase is not configured. Add SUPABASE_URL and SUPABASE_PUBLISHABLE_KEY "
                         "to this app's secrets (or environment variables).")
                return
            login_tab, signup_tab = st.tabs(["Log in", "Sign up"])
            with login_tab:
                _render_login_form()
            with signup_tab:
                _render_signup_form()


def render_account_box() -> None:
    """Top of the sidebar: user info, subscription status + upgrade button, then Logout."""
    with st.sidebar:
        name = st.session_state.get("username", "user")
        email = st.session_state.get("email", "")
        st.markdown(f"Signed in as **{name}**")
        if email:
            st.caption(email)
        render_subscription_sidebar()
        st.button("Logout", on_click=logout)
        st.divider()


# --------------------------------------------------------------------------- #
# Subscription / pricing
# --------------------------------------------------------------------------- #
def refresh_profile(force: bool = False) -> None:
    """
    Re-read the signed-in user's plan from the Supabase `users` table (at most every
    PROFILE_TTL_SECONDS unless forced). If the read fails, the previous plan is kept, so a hiccup
    never downgrades a paying user; `profile_error` lets the UI say the status may be stale.
    """
    now = time.time()
    if not force and now - st.session_state.get("profile_checked_at", 0.0) < PROFILE_TTL_SECONDS:
        return
    st.session_state["profile_checked_at"] = now

    client = st.session_state.get("sb")
    profile = backend.fetch_profile(client, st.session_state.get("user_id", "")) if client else None
    if profile is None:
        st.session_state["profile_error"] = True
        st.session_state.setdefault("plan", "free")
        return

    st.session_state["profile_error"] = False
    st.session_state["plan"] = backend.normalize_plan(profile.get("subscription_status"))
    st.session_state["stripe_customer_id"] = profile.get("stripe_customer_id")
    if profile.get("username"):
        st.session_state["username"] = profile["username"]


def get_plan() -> str:
    """'free' or 'pro', straight from the database (cached for PROFILE_TTL_SECONDS)."""
    refresh_profile()
    plan = st.session_state.get("plan", "free")
    return plan if plan in PLANS else "free"


def checkout_url() -> str:
    """
    Stripe Payment Link tagged with this user. client_reference_id (the Supabase user id, a UUID, so
    only letters/digits/dashes) comes back in the checkout.session.completed webhook, which is how
    we know WHICH account to upgrade. prefilled_email just saves them typing.
    """
    base = get_setting("STRIPE_PAYMENT_LINK", DEFAULT_PAYMENT_LINK)
    params = {}
    if st.session_state.get("user_id"):
        params["client_reference_id"] = st.session_state["user_id"]
    if st.session_state.get("email"):
        params["prefilled_email"] = st.session_state["email"]
    if not params:
        return base
    return f"{base}{'&' if '?' in base else '?'}{urlencode(params)}"


def portal_url() -> str:
    return get_setting("STRIPE_PORTAL_LINK", DEFAULT_PORTAL_LINK)


def link_button(target, label: str, url: str, primary: bool = False, full_width: bool = False) -> None:
    """Button that opens `url` in a new tab; plain link on Streamlit versions without link_button."""
    if not hasattr(target, "link_button"):
        target.markdown(f"[{label}]({url})")
        return
    kind = "primary" if primary else "secondary"
    if full_width:
        # Newer Streamlit uses width="stretch"; older versions use use_container_width=True.
        for extra in ({"width": "stretch"}, {"use_container_width": True}):
            try:
                target.link_button(label, url, type=kind, **extra)
                return
            except Exception:
                continue
    target.link_button(label, url, type=kind)


def render_subscription_sidebar() -> None:
    """Prominent card: subscription status (from the database) + Stripe upgrade button."""
    plan = get_plan()
    with st.container(border=True):
        st.markdown("**Subscription Status**")
        if plan == "free":
            st.info(PLANS["free"]["name"])
            link_button(st, UPGRADE_LABEL, checkout_url(), primary=True, full_width=True)
            st.caption("Unlock Pro features. Secure payment by Stripe.")
            st.button("Already paid? Refresh status", on_click=refresh_profile, kwargs={"force": True})
        else:
            st.success(f"{PLANS['pro']['name']} - active")
            link_button(st, "Manage subscription", portal_url(), full_width=True)
        if st.session_state.get("profile_error"):
            st.caption("Could not load your subscription status just now; showing the last known plan.")


def render_plan_card(plan_key: str, current_plan: str) -> None:
    plan = PLANS[plan_key]
    with st.container(border=True):
        st.subheader(plan["name"])
        st.markdown(f"### {plan['price']} / month")
        st.markdown("\n".join(f"- {feature}" for feature in plan["features"]))
        if plan_key == current_plan:
            st.success("Your current plan")
            if plan_key == "pro":
                link_button(st, "Manage subscription (billing portal)", portal_url())
        elif plan_key == "pro":
            link_button(st, "Choose Pro - $15/month", checkout_url(), primary=True)


def render_pricing() -> None:
    """Main-area 'Subscription & Pricing' tab."""
    plan = get_plan()
    st.subheader("Subscription & pricing")

    c1, c2 = st.columns(2)
    c1.metric("Current plan", PLANS[plan]["name"])
    c2.metric("Billing", "Free" if plan == "free" else f"{PLANS['pro']['price']} / month")

    left, right = st.columns(2)
    with left:
        render_plan_card("free", plan)
    with right:
        render_plan_card("pro", plan)

    st.caption("Payments are processed securely by Stripe. Cancel any time from the billing portal.")
    if get_setting("STRIPE_PAYMENT_LINK", DEFAULT_PAYMENT_LINK) == DEFAULT_PAYMENT_LINK:
        st.caption("Heads-up: the placeholder Stripe link is in use. Set STRIPE_PAYMENT_LINK to go live.")


# --------------------------------------------------------------------------- #
# UI sections
# --------------------------------------------------------------------------- #
def render_sidebar() -> dict:
    st.sidebar.header("Settings")
    page_cap = pl.limits_for(get_plan()).max_pages_per_keyword
    max_pages = st.sidebar.slider("Pages per keyword", 1, page_cap, min(2, page_cap),
                                  help=f"Your plan allows up to {page_cap}. "
                                       "Shopee shows roughly 60 products per page.")
    scrolls = st.sidebar.slider("Scroll rounds per page", 3, 15, 8,
                                help="More scrolling loads more lazy-loaded products.")
    if SERVER_MODE:
        use_chrome = False
        profile = DEFAULT_PROFILE
        st.sidebar.divider()
        st.sidebar.subheader("Server mode")
        st.sidebar.caption(
            "Running headless on a server: no browser window, no manual login, and a fresh browser "
            "profile each run. If Shopee blocks the session, add a saved login session via the "
            "SHOPEE_STORAGE_STATE_JSON secret (see README notes) or run the app on your own computer.")
    else:
        use_chrome = st.sidebar.checkbox("Use installed Google Chrome", value=False,
                                         help="A real Chrome install looks more natural than bundled Chromium.")
        profile = st.sidebar.text_input("Browser profile folder", DEFAULT_PROFILE,
                                        help="Keeps your Shopee login/captcha session between runs.")

        st.sidebar.divider()
        st.sidebar.subheader("Login / captcha")
        st.sidebar.caption("Shopee often blocks logged-out sessions. Log in once here; "
                           "the session is saved to the profile folder and reused by every run.")
        if st.sidebar.button("Open Shopee to log in"):
            proc = st.session_state.get("login_proc")
            if not SCRAPER.exists():
                st.sidebar.error(f"Missing {SCRAPER.name}")
            elif proc is not None and proc.poll() is None:
                st.sidebar.info("The login browser is already open.")
            else:
                cmd = [sys.executable, str(SCRAPER), "--login", "--profile", profile]
                if use_chrome:
                    cmd += ["--channel", "chrome"]
                st.session_state.login_proc = subprocess.Popen(
                    cmd, cwd=str(BASE_DIR), stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                st.sidebar.success("Browser opened. Log in, then CLOSE the window before scraping.")

    st.sidebar.divider()
    st.sidebar.caption("Automated scraping is against Shopee's terms of service. "
                       "Keep volumes low and use the data responsibly.")
    return dict(max_pages=max_pages, scrolls=scrolls, use_chrome=use_chrome, profile=profile)


def render_results() -> None:
    df: Optional[pd.DataFrame] = st.session_state.get("df")
    if df is None:
        return

    st.subheader("Results")
    prices = df["price"].dropna() if "price" in df else pd.Series(dtype=float)
    kw_count = len({k for s in df.get("keywords", pd.Series(dtype=str)).dropna()
                    for k in str(s).split(KEYWORD_SEP)})
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Products", f"{len(df):,}")
    c2.metric("Keywords", kw_count)
    c3.metric("Avg price", f"฿{prices.mean():,.0f}" if not prices.empty else "-")
    sold = df["historical_sold"].dropna() if "historical_sold" in df else pd.Series(dtype=float)
    c4.metric("Top sold", f"{int(sold.max()):,}" if not sold.empty else "-")

    view = apply_filters(df)
    st.caption(f"Showing {len(view):,} of {len(df):,} products. Click a column header to sort.")

    wanted = ["name", "price", "historical_sold", "stock", "keywords", "item_id", "shop_id", "url"]
    show_dataframe(
        view,
        hide_index=True,
        column_order=[c for c in wanted if c in view.columns],
        column_config={
            "name": st.column_config.TextColumn("Product", width="large"),
            "price": st.column_config.NumberColumn("Price (THB)", format="฿%.2f"),
            "historical_sold": st.column_config.NumberColumn("Sold", format="%d"),
            "stock": st.column_config.NumberColumn("Stock", format="%d"),
            "keywords": st.column_config.TextColumn("Keywords"),
            "item_id": st.column_config.NumberColumn("Item ID", format="%d"),
            "shop_id": st.column_config.NumberColumn("Shop ID", format="%d"),
            "url": st.column_config.LinkColumn("Link", display_text="Open"),
        },
    )

    d1, d2 = st.columns(2)
    d1.download_button(
        "Download CSV (all results)",
        data=st.session_state["csv_bytes"],
        file_name=st.session_state["csv_name"],
        mime="text/csv",
        type="primary",
    )
    if len(view) != len(df):
        d2.download_button(
            "Download CSV (filtered view)",
            data=csv_bytes_for_excel(view.drop(columns=["url"], errors="ignore")),
            file_name=st.session_state["csv_name"].replace(".csv", "_filtered.csv"),
            mime="text/csv",
        )


def verify_plan_now() -> str:
    """
    The user's plan, re-read from Supabase RIGHT NOW (not the cached session value), with the login
    token validated by Supabase Auth. If anything cannot be verified, Free applies (and the user is told).
    """
    client = st.session_state.get("sb")
    result = (backend.verify_user_and_plan(client) if client is not None
              else backend.VerifiedPlan("free", False, reason="no active session"))

    # The verified identity must be the user this session logged in as.
    if result.verified and result.user_id != st.session_state.get("user_id"):
        result = backend.VerifiedPlan("free", False, reason="session does not match the signed-in user")

    if result.verified:   # keep the sidebar/pricing views in sync with what was just verified
        st.session_state["plan"] = result.plan
        st.session_state["profile_checked_at"] = time.time()
        st.session_state["profile_error"] = False
    else:
        st.warning(f"We could not verify your subscription ({result.reason}), so Free-plan limits "
                   "apply to this run. Try 'Refresh status' or log in again.")
    return result.plan


def show_outcome(result: "runner.RunResult", csv_path: Path) -> None:
    """Show what a finished (or stopped) job produced, and tidy up its output file."""
    st.session_state["log"] = result.lines
    df = load_results(csv_path)

    if df is None:
        runner.discard_if_empty(csv_path)
        st.session_state["df"] = None
        if result.status in ("timeout", "stalled"):
            st.error(f"The run was stopped because it {result.message}, and nothing was collected.")
        elif result.status == "failed":
            st.error(f"The scraper failed ({result.message}) and collected nothing.")
        else:
            st.error("No products were collected.")
        if SERVER_MODE:
            st.info("Most often Shopee blocked the server's session (datacenter IPs are frequently flagged) "
                    "or the browser could not start. Check the run log below for details.")
        else:
            st.info("Most often Shopee blocked the session. Click **Open Shopee to log in** in the sidebar, "
                    "log in, close that window, then run again. Check the run log below for details.")
        return

    st.session_state["df"] = df
    st.session_state["csv_bytes"] = csv_path.read_bytes()     # original file, BOM included
    st.session_state["csv_name"] = csv_path.name
    if result.ok:
        st.success(f"Collected {len(df):,} unique products in {format_duration(result.elapsed)}.")
    else:
        st.warning(f"The run was stopped because it {result.message}. Showing the {len(df):,} products "
                   "collected before that (partial results).")

    text = "\n".join(result.lines)
    if "[action]" in text or "skipped by user" in text or "blocked" in text.lower():
        st.warning("Some pages were blocked or skipped. Log in via the sidebar and re-run "
                   "for more complete results.")


def start_scrape(keywords: List[str], settings: dict) -> None:
    # 1) PLAN LIMITS, before anything is reserved or started: fresh plan from Supabase, then
    #    plan_limits.validate_job(). Nothing below runs if the job is over quota.
    plan = verify_plan_now()
    check = pl.validate_job(plan, keywords, settings["max_pages"])
    if not check.ok:
        st.error("This run was not started because it exceeds your plan limits:\n\n"
                 + "\n".join(f"- {message}" for message in check.errors))
        if check.plan == "free":
            link_button(st, "Upgrade to Pro for higher limits", checkout_url())
        return

    user_id = st.session_state.get("user_id", "")
    if not user_id:
        st.error("Your session is incomplete. Please log in again.")
        return

    # 2) ONE JOB PER USER and a server-wide cap on simultaneous browsers.
    busy_reason = runner.JOBS.acquire(user_id, max_concurrent_scrapes())
    if busy_reason:
        st.warning(busy_reason)
        return

    spinner_text = ("Scraping Shopee in a headless browser on the server... the first run may need a "
                    "one-time browser install."
                    if SERVER_MODE else
                    "Scraping Shopee... a browser window will open. Please leave it open.")
    try:
        # 3) PER-USER, PER-JOB OUTPUT FILE: dashboard_output/<user-id>/shopee_products_<time>_<random>.csv
        OUTPUT_DIR.mkdir(exist_ok=True)
        csv_path = runner.new_output_path(OUTPUT_DIR, user_id)
        runner.prune_outputs(csv_path.parent, OUTPUT_KEEP_PER_USER, OUTPUT_MAX_AGE_SECONDS)

        # 4) RUN with a runtime limit and a hang limit; the runner always cleans up the process tree.
        with st.spinner(spinner_text):
            result = run_scraper(
                keywords, settings["max_pages"], settings["scrolls"],
                settings["profile"], settings["use_chrome"], csv_path, check.plan)
    except pl.PlanLimitError as exc:       # cannot normally happen: validated just above
        st.error(str(exc))
        return
    except OSError as exc:
        log.exception("could not prepare or start the scrape job")
        st.error(f"Could not start the job: {exc}")
        return
    except Exception:
        log.exception("unexpected error while scraping")
        st.error("Something went wrong while scraping. Please try again.")
        return
    finally:
        runner.JOBS.release(user_id)       # always give the slot back (success, failure, timeout, stop)

    show_outcome(result, csv_path)


def render_scraper(settings: dict) -> None:
    """The scraper dashboard: keyword picker, Run button, progress, results, downloads, log."""
    if not SCRAPER.exists():
        st.error(f"Could not find `{SCRAPER.name}` next to app.py. Put both files in the same folder.")
        return

    with st.container(border=True):
        st.subheader("1. Choose keywords")
        selected = st.multiselect("Popular keywords", PRESET_KEYWORDS, default=DEFAULT_SELECTION)
        custom = st.text_input("Add your own (comma-separated)",
                               placeholder="e.g. ครีมกันแดด, lip tint, toner")
        keywords = collect_keywords(selected, custom)

        pages_total = len(keywords) * settings["max_pages"]
        if keywords:
            max_run = runner.compute_timeouts(len(keywords), settings["max_pages"])[0]
            st.caption(f"{len(keywords)} keyword(s) x up to {settings['max_pages']} page(s) = "
                       f"up to {pages_total} page loads (roughly {max(1, pages_total * 40 // 60)}+ minutes). "
                       f"A run is stopped automatically after {max_run // 60} min.")
        plan_now = get_plan()
        st.caption(f"{PLANS[plan_now]['name']}: {pl.describe(plan_now)}.")
        preview = pl.validate_job(plan_now, keywords, settings["max_pages"])
        if keywords and not preview.ok:
            st.warning("\n".join(preview.errors))   # advisory only; the real check runs on click
        run_clicked = st.button("Run Scraper", type="primary", disabled=not keywords)

    if run_clicked:
        login_proc = st.session_state.get("login_proc")
        if login_proc is not None and login_proc.poll() is None:
            st.warning("The login browser is still open. Close that window first, then run again "
                       "(the profile can only be used by one browser at a time).")
        else:
            start_scrape(keywords, settings)

    render_results()

    if st.session_state.get("log"):
        with st.expander("Run log"):
            st.code("\n".join(st.session_state["log"][-200:]), language=None)


def main() -> None:
    st.set_page_config(page_title="Shopee TH Scraper", page_icon="🛍️", layout="wide")
    st.markdown(
        """
        <style>
        .block-container {padding-top: 2rem; max-width: 1300px;}
        [data-testid="stMetric"] {
            background: rgba(128,128,128,0.08);
            border: 1px solid rgba(128,128,128,0.2);
            padding: 14px 16px; border-radius: 12px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # ---- authentication gate: nothing below this point is shown to signed-out visitors ----
    if not require_login():
        return

    # ---- sidebar: user info, subscription (upgrade), logout, then scraper settings ----
    render_account_box()      # user info, subscription status + upgrade button, logout
    settings = render_sidebar()

    # ---- main area ----
    st.title("🛍️ Shopee Thailand Product Scraper")
    st.caption(f"Pick keywords, run the scraper, then explore and download the results. "
               f"Plan: {PLANS[get_plan()]['name']}")

    scraper_tab, pricing_tab = st.tabs(["🛍️ Scraper", "💎 Subscription & Pricing"])
    with scraper_tab:
        render_scraper(settings)
    with pricing_tab:
        render_pricing()


if __name__ == "__main__":
    main()