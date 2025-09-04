import csv
import random
import time
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from collections import deque

START_URL = "[URL for the product page you want to scrape]"
OUT = "[csv Output file name with extesnion]"

ALLOWED_PREFIX = "[Category link]"  # keep inside this category only
TIMEOUT = 25
RETRIES = 6
SLEEP_RANGE = (0.6, 1.4)
FETCH_H1_FOR_TITLE = True  # set False to skip opening each product page (faster)

HEADERS = [
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15"},
    {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"},
]

def is_listing_url(url: str) -> bool:
    u = urlparse(url)
    return u.netloc in {"simplybearings.co.uk", "www.simplybearings.co.uk"} and u.path.startswith(ALLOWED_PREFIX)

def is_product_url(url: str) -> bool:
    u = urlparse(url)
    return "/shop/p" in u.path and u.path.endswith("/product_info.html")

def fetch(url: str) -> requests.Response | None:
    last = None
    for attempt in range(1, RETRIES + 1):
        time.sleep(random.uniform(*SLEEP_RANGE))
        try:
            r = requests.get(url, headers=random.choice(HEADERS), timeout=TIMEOUT)
            if r.status_code == 200:
                return r
            if r.status_code in (429, 502, 503, 504):
                time.sleep(0.8 * attempt)
        except requests.RequestException as e:
            last = e
            time.sleep(0.6 * attempt)
    print(f"[WARN] fetch failed: {url} last={last}")
    return None

def main_content(soup: BeautifulSoup):
    for sel in ["#content", "#maincontent", "#CenterPanel", "div#content", "div#main", "div.main", "#center", ".col-md-9", ".content"]:
        el = soup.select_one(sel)
        if el:
            return el
    return soup

def extract_product_links(listing_html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(listing_html, "lxml")
    content = main_content(soup)
    links = set()
    for a in content.select('a[href*="/shop/p"][href$="/product_info.html"]'):
        href = urljoin(base_url, a.get("href", ""))
        if is_product_url(href):
            links.add(href)
    return sorted(links)

def extract_pagination_targets(listing_html: str, base_url: str) -> list[str]:
    """
    Parse ALL pagination links (numbers, arrows, ») from the pager area and return absolute URLs.
    We then BFS through these, deduping with a set.
    """
    soup = BeautifulSoup(listing_html, "lxml")
    content = main_content(soup)

    pager = []
    # common pager containers
    containers = content.select(".pagination, .pager, nav[role='navigation'], .pages, #productListingTop, #productListingBottom")
    if not containers:
        containers = [content]  # fallback: scan content

    for container in containers:
        for a in container.select("a[href]"):
            txt = (a.get_text() or "").strip().lower()
            if txt.isdigit() or txt in {"next", "»", "›"} or "page" in a.get("href", "").lower():
                href = urljoin(base_url, a["href"])
                if is_listing_url(href):
                    pager.append(href)

    # dedupe while preserving order
    seen = set()
    out = []
    for u in pager:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def get_product_title(product_url: str) -> str:
    r = fetch(product_url)
    if not r:
        return ""
    soup = BeautifulSoup(r.text, "lxml")
    h1 = soup.find("h1")
    return h1.get_text(strip=True) if h1 else ""

def crawl_all():
    seen_pages = set()
    seen_products = set()
    q = deque([START_URL])

    # prepare CSV header
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(["title", "url"])

    total_saved = 0

    while q:
        page = q.popleft()
        if page in seen_pages or not is_listing_url(page):
            continue
        seen_pages.add(page)
        print(f"[INFO] listing page {len(seen_pages)}: {page}")

        r = fetch(page)
        if not r:
            continue

        # 1) products on this page
        prod_urls = extract_product_links(r.text, r.url)
        print(f"       -> found {len(prod_urls)} products")

        # 2) write out (and fetch titles if enabled)
        with open(OUT, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for u in prod_urls:
                if u in seen_products:
                    continue
                title = get_product_title(u) if FETCH_H1_FOR_TITLE else ""
                w.writerow([title, u])
                seen_products.add(u)
                total_saved += 1

        # 3) enqueue all other pages from the pager
        next_pages = extract_pagination_targets(r.text, r.url)
        # keep only unseen listing URLs
        for np in next_pages:
            if np not in seen_pages and is_listing_url(np):
                q.append(np)

        print(f"       -> pages queued: {len(q)}   total products saved: {total_saved}")

    print(f"[DONE] pages crawled: {len(seen_pages)} | products saved: {total_saved} | file: {OUT}")

if __name__ == "__main__":
    crawl_all()
