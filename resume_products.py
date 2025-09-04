import logging
from pathlib import Path
import scrapy
from scrapy import Request

from sb_scraper.utils import (
    strip_query,
    normalize_product_url,
    load_urls_any,
    _read_csv_urls,
    _read_jsonl_like_urls,
    _read_txt_urls,
)

logger = logging.getLogger(__name__)


class ResumeProductsSpider(scrapy.Spider):
    name = "resume_products"

    custom_settings = {
        # keep your other settings in settings.py
    }

    def __init__(self, product_list_csv: str, existing_csv: str,
                 url_column: str = "url", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.product_list_csv = product_list_csv
        self.existing_csv = existing_csv
        self.url_column = url_column

        self._known: set[str] = set()      # known product URLs (normalised)
        self._candidates: list[str] = []   # input URLs (cats or products)

    # --- REQUIRED to fix jobdir restore ---
    def errback(self, failure):
        self.logger.warning("[errback] %s", failure.getErrorMessage())

    def start_requests(self):
        root = Path().resolve()

        # 1) Load candidate inputs (can be category URLs or product URLs)
        candidate_inputs = [str((root / p.strip())) for p in self.product_list_csv.split(",")]
        raw_candidates = load_urls_any(candidate_inputs, self.url_column)
        # Normalise for product matching later (ok if they are categories too)
        self._candidates = [u.strip() for u in raw_candidates if u.strip()]

        # 2) Build known set from prior outputs (files or directories, recursive)
        existing_inputs = [str((root / p.strip())) for p in self.existing_csv.split(",")]

        known: set[str] = set()
        for raw in existing_inputs:
            p = Path(raw)
            if not p.exists():
                logger.warning("[resume] Missing existing source: %s", p)
                continue
            if p.is_dir():
                for f in p.rglob("*"):
                    known |= self._collect_known_from_file(f)
            else:
                known |= self._collect_known_from_file(p)

        self._known = known

        logger.info("[resume] Loaded %d candidate URLs", len(self._candidates))
        logger.info("[resume] Loaded %d already-scraped URLs", len(self._known))

        # 3) Queue candidates. We detect category vs product in parse_entry.
        #    Use dont_filter=True so jobdir restores don’t get dupefiltered.
        for url in self._candidates:
            yield Request(url, callback=self.parse_entry, errback=self.errback, dont_filter=True)

    def _collect_known_from_file(self, path: Path) -> set[str]:
        urls: list[str] = []
        sfx = path.suffix.lower()
        if sfx == ".csv":
            urls = _read_csv_urls(path, self.url_column)
        elif sfx in (".jsonl", ".jl"):
            urls = _read_jsonl_like_urls(path, "url")
        elif sfx == ".txt":
            urls = _read_txt_urls(path)
        else:
            return set()

        # normalise to product form (strip query/fragment)
        return {normalize_product_url(u) for u in urls if u}

    # Decide if a URL/page is product or category and route accordingly
    def parse_entry(self, response: scrapy.http.Response):
        url = response.url
        if self._looks_like_product_url(url) or self._is_product_page(response):
            # Direct product URL
            return (yield from self.parse_product(response))

        # Otherwise treat as a category/listing page
        yield from self.parse_category(response)

    # --- Heuristics (tweak for your site) ---
    def _looks_like_product_url(self, url: str) -> bool:
        u = url.lower()
        # ZenCart-like pattern: products_id=
        return ("products_id=" in u) or u.rstrip("/").endswith(".html")

    def _is_product_page(self, response: scrapy.http.Response) -> bool:
        # Look for typical add-to-cart or product detail selectors
        if response.css('form[name="cart_quantity"], form[action*="add_product"]'):
            return True
        # A unique product title area (site-specific selector — adjust if needed)
        if response.css("h1.productGeneral, h1#productName"):
            return True
        return False

    # Parse a category/listing: collect product links and (optional) pagination
    def parse_category(self, response: scrapy.http.Response):
        # Skip empty categories
        if b"Sorry, no product found" in response.body or \
           response.css("div:contains('Sorry, no product found')"):
            self.logger.debug("[category] empty: %s", response.url)
            return

        # Extract product links — adjust the selectors to your site:
        product_hrefs = set()

        # 1) Common ZenCart pattern
        product_hrefs.update(response.css('a[href*="products_id="]::attr(href)').getall())
        # 2) Generic product link classes (tweak to your DOM)
        product_hrefs.update(response.css('a.productlisting-link::attr(href)').getall())
        product_hrefs.update(response.css('a.product-name::attr(href)').getall())

        # Normalise and filter against known
        to_visit = []
        for href in product_hrefs:
            u = response.urljoin(href)
            nu = normalize_product_url(u)
            if nu and nu not in self._known:
                to_visit.append(nu)

        # Request unseen products
        for u in to_visit:
            yield Request(u, callback=self.parse_product, errback=self.errback, dont_filter=True)

        # Follow pagination (tweak selectors for your theme)
        next_links = response.css('a:contains("Next")::attr(href), a.next::attr(href), li.pagination-next a::attr(href)').getall()
        for nl in next_links:
            yield Request(response.urljoin(nl), callback=self.parse_category, errback=self.errback, dont_filter=True)

    def parse_product(self, response: scrapy.http.Response):
        url = normalize_product_url(response.url)

        # Mark as known to avoid re-requesting from other categories
        self._known.add(url)

        # Minimal item; extend with real fields
        yield {
            "url": url,
            "status": response.status,
            "title": response.css("h1::text").get() or response.css("title::text").get(),
        }
