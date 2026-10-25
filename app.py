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

The dashboard sits behind a simple login (test credentials: user / shopee123; override with the
APP_USERNAME and APP_PASSWORD environment variables or Streamlit secrets).

On your own computer the scraper opens a real browser window; on a server such as Streamlit Cloud
it automatically runs headless with container-safe flags (see shopee_scraper_multi.py).
"""

import hmac
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st

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

# Simple demo credentials for testing. For a real deployment set APP_USERNAME / APP_PASSWORD as
# environment variables or Streamlit secrets instead of relying on these defaults.
DEFAULT_USERNAME = "user"
DEFAULT_PASSWORD = "shopee123"
SESSION_KEYS_TO_CLEAR = ("authenticated", "username", "plan", "df", "csv_bytes", "csv_name", "log")

# ---- Subscription / pricing (Stripe) ---------------------------------------------------------
# Replace the placeholders below (or set STRIPE_PAYMENT_LINK / STRIPE_PORTAL_LINK as env vars or
# Streamlit secrets). The plan copy is placeholder text: edit PLANS to match what you really sell.
DEFAULT_PAYMENT_LINK = "https://buy.stripe.com/test_placeholder"
DEFAULT_PORTAL_LINK = "https://billing.stripe.com/p/login/test_placeholder"
UPGRADE_LABEL = "Upgrade to Pro ($15/month)"
PLANS = {
    "free": {
        "name": "Free Tier",
        "price": "$0",
        "features": [
            "Multi-keyword Shopee search",
            "Interactive results table with filters",
            "CSV export",
        ],
    },
    "pro": {
        "name": "Pro Tier",
        "price": "$15",
        "features": [
            "Everything in Free",
            "Higher page and keyword limits (coming soon)",
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
                  use_chrome: bool, output_csv: Path) -> List[str]:
    cmd = [
        sys.executable, "-u", str(SCRAPER),
        "--keywords", *keywords,
        "--max-pages", str(max_pages),
        "--scrolls", str(scrolls),
        "--retries", "2",
        "--profile", profile,
        "--output", str(output_csv),
        "--no-print-json",
    ]
    if use_chrome:
        cmd += ["--channel", "chrome"]
    return cmd


def run_scraper(keywords: List[str], max_pages: int, scrolls: int, profile: str,
                use_chrome: bool, output_csv: Path) -> Tuple[int, List[str]]:
    """
    Run the scraper as a subprocess, streaming its output into a progress bar and log box.
    Returns (return_code, all_log_lines).
    """
    env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"}  # Thai text on Windows
    cmd = build_command(keywords, max_pages, scrolls, profile, use_chrome, output_csv)

    progress = st.progress(0.0, text="Starting the browser...")
    log_box = st.empty()

    lines: List[str] = []
    fraction, message, unique_total = 0.0, "Starting the browser...", 0

    proc = subprocess.Popen(
        cmd, cwd=str(BASE_DIR), env=env,
        stdin=subprocess.DEVNULL,                  # non-interactive: blocked pages are skipped
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace", bufsize=1,
    )
    try:
        assert proc.stdout is not None
        for raw in proc.stdout:
            line = raw.rstrip()
            if not line:
                continue
            lines.append(line)

            if m := RE_PAGE.search(line):
                k, total_k, kw, page, total_p = m.groups()
                fraction = ((int(k) - 1) + (int(page) - 1) / int(total_p)) / int(total_k)
                message = f"Keyword {k}/{total_k} '{kw}' - page {page}/{total_p}"
            elif m := RE_DONE.search(line):
                k, total_k, kw, count = m.group(1), m.group(2), m.group(3), m.group(4)
                fraction = int(k) / int(total_k)
                message = f"Finished '{kw}' ({count} products)"
            elif "Waiting" in line and "before" in line:
                message = "Pausing between requests (keeps the scraper polite)..."
            elif "[scroll]" in line:
                message = message.split(" - scrolling")[0] + " - scrolling"
            if m := RE_TOTAL.search(line):
                unique_total = int(m.group(1))

            suffix = f" | {unique_total} unique products so far" if unique_total else ""
            progress.progress(min(max(fraction, 0.0), 1.0), text=message + suffix)
            log_box.code("\n".join(lines[-LOG_LINES_SHOWN:]), language=None)
        proc.wait()
    finally:
        if proc.poll() is None:                    # e.g. Streamlit interrupted this run
            proc.kill()

    done = proc.returncode == 0
    progress.progress(1.0 if done else min(fraction, 1.0),
                      text="Scraping complete" if done else "Scraper exited with an error")
    return proc.returncode, lines


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
# Authentication (basic login/logout backed by st.session_state)
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


def get_credentials() -> Tuple[str, str]:
    """Expected (username, password): env vars / Streamlit secrets if set, else the test defaults."""
    return get_setting("APP_USERNAME", DEFAULT_USERNAME), get_setting("APP_PASSWORD", DEFAULT_PASSWORD)


def check_credentials(username: str, password: str) -> bool:
    """Constant-time comparison so response timing does not leak how much of a guess matched."""
    expected_user, expected_pass = get_credentials()
    user_ok = hmac.compare_digest((username or "").strip().encode("utf-8"), expected_user.encode("utf-8"))
    pass_ok = hmac.compare_digest((password or "").encode("utf-8"), expected_pass.encode("utf-8"))
    return user_ok and pass_ok


def is_authenticated() -> bool:
    return bool(st.session_state.get("authenticated", False))


def rerun() -> None:
    """st.rerun() on current Streamlit, st.experimental_rerun() on older versions."""
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.experimental_rerun()


def logout() -> None:
    """Sign out and drop everything held for this session (results, CSV bytes, run log)."""
    for key in SESSION_KEYS_TO_CLEAR:
        st.session_state.pop(key, None)


def render_login() -> None:
    """Centered login form shown instead of the dashboard while signed out."""
    st.markdown("<style>[data-testid='stSidebar'] {display: none;}</style>", unsafe_allow_html=True)
    _, middle, _ = st.columns([1, 1.4, 1])
    with middle:
        st.write("")
        with st.container(border=True):
            st.title("🛍️ Shopee Scraper")
            st.caption("Sign in to continue.")
            with st.form("login_form"):
                username = st.text_input("Username")
                password = st.text_input("Password", type="password")
                submitted = st.form_submit_button("Log in", type="primary")

            if submitted:
                if check_credentials(username, password):
                    st.session_state["authenticated"] = True
                    st.session_state["username"] = username.strip()
                    st.session_state["plan"] = lookup_plan(username)
                    st.session_state["failed_logins"] = 0
                    rerun()
                else:
                    st.session_state["failed_logins"] = st.session_state.get("failed_logins", 0) + 1
                    time.sleep(min(1 + st.session_state["failed_logins"], 5))  # slow down guessing
                    st.error("Invalid username or password.")

            if get_credentials() == (DEFAULT_USERNAME, DEFAULT_PASSWORD):
                st.caption(f"Test login: `{DEFAULT_USERNAME}` / `{DEFAULT_PASSWORD}`")


def render_account_box() -> None:
    """Top of the sidebar: user info, subscription status + upgrade button, then Logout."""
    with st.sidebar:
        st.markdown(f"Signed in as **{st.session_state.get('username', 'user')}**")
        render_subscription_sidebar()
        st.button("Logout", on_click=logout)
        st.divider()


# --------------------------------------------------------------------------- #
# Subscription / pricing
# --------------------------------------------------------------------------- #
def lookup_plan(username: str) -> str:
    """
    Which plan a user is on: "free" or "pro".

    DEMO ONLY: users listed in APP_PRO_USERS (comma-separated, env var or secret) count as Pro, so
    you can preview both states. In production, replace this with a lookup of the user's real
    subscription status, written by a Stripe webhook (checkout.session.completed /
    customer.subscription.updated / customer.subscription.deleted) into your database.
    """
    pro_users = {u.strip().lower() for u in get_setting("APP_PRO_USERS").split(",") if u.strip()}
    return "pro" if (username or "").strip().lower() in pro_users else "free"


def get_plan() -> str:
    plan = st.session_state.get("plan")
    if plan not in PLANS:
        plan = lookup_plan(st.session_state.get("username", ""))
        st.session_state["plan"] = plan
    return plan


def checkout_url() -> str:
    """Stripe Payment Link with the signed-in user attached as client_reference_id, so a webhook
    can tell which account paid (Stripe allows only letters, digits, '-' and '_' there)."""
    base = get_setting("STRIPE_PAYMENT_LINK", DEFAULT_PAYMENT_LINK)
    ref = re.sub(r"[^A-Za-z0-9_-]", "_", st.session_state.get("username", ""))[:200]
    if not ref:
        return base
    return f"{base}{'&' if '?' in base else '?'}client_reference_id={ref}"


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
    """Prominent card: subscription status + full-width Stripe upgrade button (call inside the sidebar)."""
    plan = get_plan()
    with st.container(border=True):
        st.markdown("**Subscription Status**")
        if plan == "free":
            st.info(PLANS["free"]["name"])
            link_button(st, UPGRADE_LABEL, checkout_url(), primary=True, full_width=True)
            st.caption("Unlock Pro features. Secure payment by Stripe.")
        else:
            st.success(f"{PLANS['pro']['name']} - active")
            link_button(st, "Manage subscription", portal_url(), full_width=True)


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
    max_pages = st.sidebar.slider("Pages per keyword", 1, 10, 2,
                                  help="Shopee shows roughly 60 products per page.")
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


def start_scrape(keywords: List[str], settings: dict) -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    csv_path = OUTPUT_DIR / f"shopee_products_{datetime.now():%Y%m%d_%H%M%S}.csv"

    spinner_text = ("Scraping Shopee in a headless browser on the server... the first run may need a "
                    "one-time browser install."
                    if SERVER_MODE else
                    "Scraping Shopee... a browser window will open. Please leave it open.")
    with st.spinner(spinner_text):
        return_code, lines = run_scraper(
            keywords, settings["max_pages"], settings["scrolls"],
            settings["profile"], settings["use_chrome"], csv_path)

    st.session_state["log"] = lines
    df = load_results(csv_path)

    if df is None:
        st.session_state["df"] = None
        st.error("No products were collected." if return_code == 0
                 else f"The scraper exited with code {return_code} and collected nothing.")
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
    st.success(f"Collected {len(df):,} unique products. Saved to dashboard_output/{csv_path.name}")

    text = "\n".join(lines)
    if "[action]" in text or "skipped by user" in text or "blocked" in text.lower():
        st.warning("Some pages were blocked or skipped. Log in via the sidebar and re-run "
                   "for more complete results.")


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
            st.caption(f"{len(keywords)} keyword(s) x up to {settings['max_pages']} page(s) = "
                       f"up to {pages_total} page loads (roughly {max(1, pages_total * 40 // 60)}+ minutes).")
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
    if not is_authenticated():
        render_login()
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
