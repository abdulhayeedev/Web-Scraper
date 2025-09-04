import csv
from urllib.parse import urlparse
import scrapy
from scrapy.http import HtmlResponse


# ── filters: skip non-HTML & static assets ─────────────────────────────────────
BAD_EXTS = {
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico",
    ".pdf", ".css", ".js", ".zip", ".mp4", ".mp3", ".webm",
    ".woff", ".woff2", ".ttf", ".eot",
}

def is_html(response) -> bool:
    if not isinstance(response, HtmlResponse):
        return False
    ctype = response.headers.get(b"content-type", b"").lower()
    return b"text/html" in ctype or b"application/xhtml" in ctype

def should_follow(url: str) -> bool:
    path = urlparse(url).path.lower()
    return not any(path.endswith(ext) for ext in BAD_EXTS)


class SimplyBearingsResumeSpider(scrapy.Spider):
    name = "simplybearings_resume"
    allowed_domains = ["simplybearings.co.uk"]
    start_urls = ["https://simplybearings.co.uk/shop/"]

    custom_settings = {
        "TELNETCONSOLE_ENABLED": False,
        "COOKIES_ENABLED": False,
        # uncomment if you created sb_scraper/dupe.py
        # "DUPEFILTER_CLASS": "sb_scraper.dupe.CanonicalNoQueryDupeFilter",
    }

    def __init__(self, existing_csv: str = "", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.known_urls = set()
        if existing_csv:
            try:
                with open(existing_csv, newline="", encoding="utf-8") as f:
                    r = csv.DictReader(f)
                    # accept header variants
                    for row in r:
                        u = (row.get("url") or row.get("URL") or row.get("product_url") or "").strip()
                        if u:
                            self.known_urls.add(u)
                self.logger.info("Known product URLs loaded: %d", len(self.known_urls))
            except Exception as e:
                self.logger.warning("Could not read existing_csv=%s (%s)", existing_csv, e)

    # ── crawl router ───────────────────────────────────────────────────────────
    def parse(self, response):
        if not is_html(response):
            return

        # drill down: categories / lists / products
        for href in response.css("a::attr(href)").getall():
            url = response.urljoin(href)
            if not should_follow(url):
                continue

            if "/product_info.html" in url:
                if url not in self.known_urls:
                    yield response.follow(url, callback=self.parse_product)
                continue

            # treat list/category pages
            if "/shop/" in url and ("index.html" in url or "/c" in url):
                yield response.follow(url, callback=self.parse_list)

    def parse_list(self, response):
        if not is_html(response):
            return

        # skip empty categories
        body_text = " ".join(response.css("body *::text").getall()).lower()
        if "sorry, no product found" in body_text:
            return

        # products on this page
        for a in response.css("a[href*='/product_info.html']::attr(href)").getall():
            url = response.urljoin(a)
            if not should_follow(url) or url in self.known_urls:
                continue
            yield response.follow(url, callback=self.parse_product)

        # go deeper to other list pages
        for href in response.css("a::attr(href)").getall():
            url = response.urljoin(href)
            if not should_follow(url):
                continue
            if "/shop/" in url and ("index.html" in url or "/c" in url) and "product_info.html" not in url:
                yield response.follow(url, callback=self.parse_list)

    def parse_product(self, response):
        if not is_html(response):
            return

        # basic fields — swap to your real selectors as needed
        title = response.css("h1::text, h1 *::text").get(default="").strip()
        sku = (
            response.css("div:contains('SKU'), span:contains('SKU')::text").re_first(r"SKU[:\s]*([A-Za-z0-9\-\._/]+)")
            or ""
        ).strip()
        price = (response.css(".productSpecialPrice::text, .productPrice::text, .price::text").get() or "").strip()
        image = response.css("#productMainImage::attr(src), .product_image img::attr(src)").get()

        yield {
            "url": response.url,
            "title": title,
            "sku": sku,
            "price_raw": price,
            "image": response.urljoin(image) if image else None,
        }
