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

Log in once by hand (session is saved to the profile):
    python shopee_scraper_multi.py --login

Close any other browser window that uses the same profile folder before running.
"""

import argparse
import csv
import json
import random
import re
import sys
from collections import deque
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

    try:
        with open(filename, "w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(products)
    except PermissionError:
        print(f"[error] Cannot write '{filename}'. Is it open in Excel? Close it and run again.",
              file=sys.stderr)
        return 0
    except OSError as exc:
        print(f"[error] Could not write '{filename}': {exc!r}", file=sys.stderr)
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


def ask(message: str) -> str:
    """Prompt the user; returns '' for Enter, or the lowercase answer. EOF counts as 'skip'."""
    try:
        return input(message).strip().lower()
    except EOFError:
        return "s"


def launch_context(pw, profile_dir: str, channel: Optional[str]) -> BrowserContext:
    Path(profile_dir).mkdir(parents=True, exist_ok=True)
    kwargs: Dict[str, Any] = dict(
        user_data_dir=str(Path(profile_dir).resolve()),
        headless=False,
        args=LAUNCH_ARGS,
        ignore_default_args=["--enable-automation"],
        user_agent=USER_AGENT,
        viewport=VIEWPORT,
        locale=LOCALE,
        timezone_id=TIMEZONE,
        extra_http_headers={"Accept-Language": "th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7"},
    )
    if channel:
        kwargs["channel"] = channel  # e.g. "chrome" to use the installed Google Chrome
    return pw.chromium.launch_persistent_context(**kwargs)


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
def scrape_all(keywords: List[str], max_pages: int, profile_dir: str, scroll_rounds: int,
               retries: int, channel: Optional[str], debug: bool, output_csv: str) -> ProductStore:
    store = ProductStore()
    captured: List[Any] = []
    total_keywords = len(keywords)

    with sync_playwright() as pw:
        try:
            context = launch_context(pw, profile_dir, channel)
        except Exception as exc:
            print(f"[error] Could not launch the persistent context: {exc!r}\n"
                  "        Close any other browser using this profile folder and try again.",
                  file=sys.stderr)
            return store

        context.add_init_script(STEALTH_JS)
        context.on("response", make_response_handler(captured))  # covers every page/tab
        page = context.pages[0] if context.pages else context.new_page()

        try:
            for k_idx, keyword in enumerate(keywords, 1):
                print(f"\n{'=' * 70}\n[keyword {k_idx}/{total_keywords}] '{keyword}' "
                      f"(up to {max_pages} pages)\n{'=' * 70}")
                stop_all = False

                for page_idx in range(max_pages):
                    print(f"[keyword {k_idx}/{total_keywords}] '{keyword}' - "
                          f"page {page_idx + 1}/{max_pages}  ->  {build_search_url(keyword, page_idx)}")
                    products, status = scrape_page(
                        page, captured, keyword, page_idx, scroll_rounds, retries, debug)

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
            print(f"[error] Unexpected browser error: {exc!r}", file=sys.stderr)
        finally:
            context.close()  # flushes cookies/session into the profile folder

    return store


# --------------------------------------------------------------------------- #
# Manual login helper (used by the Streamlit dashboard's "Open Shopee to log in" button)
# --------------------------------------------------------------------------- #
def login_session(profile_dir: str, channel: Optional[str]) -> None:
    """
    Open Shopee in the persistent profile so you can log in / solve a captcha by hand.
    The session is saved to the profile folder; close the browser window when you are done.
    """
    with sync_playwright() as pw:
        try:
            context = launch_context(pw, profile_dir, channel)
        except Exception as exc:
            print(f"[error] Could not launch the persistent context: {exc!r}\n"
                  "        Close any other browser using this profile folder and try again.",
                  file=sys.stderr)
            return
        context.add_init_script(STEALTH_JS)
        page = context.pages[0] if context.pages else context.new_page()
        try:
            page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60_000)
        except PlaywrightTimeoutError:
            print("[warn] Navigation timed out; continuing.", file=sys.stderr)
        print("[info] Log in / solve any captcha, then CLOSE the browser window to save the session.")
        try:
            context.wait_for_event("close", timeout=0)  # returns when the window is closed
        except KeyboardInterrupt:
            pass
        except Exception:
            pass
        finally:
            try:
                context.close()
            except Exception:
                pass


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape Shopee Thailand search results for multiple keywords with pagination.")
    parser.add_argument("--keywords", nargs="+", default=DEFAULT_KEYWORDS,
                        help=f"Keywords to scrape (default: {' '.join(DEFAULT_KEYWORDS)})")
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES,
                        help="Maximum result pages per keyword")
    parser.add_argument("--scrolls", type=int, default=8, help="Scroll rounds per page")
    parser.add_argument("--retries", type=int, default=2,
                        help="Attempts per page (with a manual-fix pause between attempts)")
    parser.add_argument("--profile", default=DEFAULT_PROFILE_DIR, help="User data dir")
    parser.add_argument("--channel", default=None, help='Browser channel, e.g. "chrome"')
    parser.add_argument("--output", default=DEFAULT_OUTPUT_CSV, help="Combined CSV output file")
    parser.add_argument("--login", action="store_true",
                        help="Only open Shopee in the profile for manual login/captcha, then exit")
    parser.add_argument("--no-print-json", action="store_true",
                        help="Do not print the full product list as JSON")
    parser.add_argument("--debug", action="store_true",
                        help=f"Print payload structure and save raw JSON to ./{DEBUG_DIR}/")
    args = parser.parse_args()

    if args.login:
        login_session(args.profile, args.channel)
        return

    keywords = list(dict.fromkeys(k.strip() for k in args.keywords if k.strip()))
    if not keywords:
        parser.error("Provide at least one non-empty keyword.")
    max_pages = max(1, args.max_pages)

    print(f"[info] Keywords: {keywords}")
    print(f"[info] Max pages per keyword: {max_pages} | output: {args.output}")

    store = scrape_all(keywords, max_pages, args.profile, args.scrolls, max(1, args.retries),
                       args.channel, args.debug, args.output)
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