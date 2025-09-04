import re
from urllib.parse import urljoin, urlparse
import scrapy
from bs4 import BeautifulSoup

from sb_scraper.items import ProductItem
# from sb_scraper.utils import load_google_sheet_urls, load_local_csv_urls

PRODUCT_RE = re.compile(r"/shop/.*product_info\.html", re.I)
SITE = "https://simplybearings.co.uk"
SHOP_ROOT = "https://simplybearings.co.uk/shop/"

class SimplyBearingsResumeSpider(scrapy.Spider):
    name = "simplybearings_sitemap"
    allowed_domains = ["simplybearings.co.uk", "www.simplybearings.co.uk"]
    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "DOWNLOAD_DELAY": 1.0,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1.0,
        # pipeline for dedupe
        "ITEM_PIPELINES": {
            "sb_scraper.pipelines.DedupeExistingPipeline": 100,
        },
    }

    def __init__(self, sheet_id="", gid="", existing_csv="", existing_csv2="", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sheet_id = sheet_id
        self.gid = gid
        self.existing_csv = existing_csv
        self.existing_csv2 = existing_csv2
        self.known_urls = set()

    def start_requests(self):
        # 1) Collect already-scraped product URLs
        urls = set()
        urls |= load_google_sheet_urls(self.sheet_id, self.gid)
        urls |= load_local_csv_urls(self.existing_csv, self.existing_csv2)
        self.known_urls = urls
        # Make available to pipelines via settings (Scrapy can't mutate settings at runtime,
        # so we pass through crawler.settings in 'from_crawler')
        self.crawler.settings.set("KNOWN_PRODUCT_URLS", self.known_urls, priority="spider")

        self.logger.info("Known product URLs loaded: %d", len(self.known_urls))
        yield scrapy.Request(SHOP_ROOT, callback=self.parse_list, dont_filter=True)

    def parse_list(self, response):
        # Discover product and crawl links within /shop/
        for a in response.css("a::attr(href)").getall():
            href = response.urljoin(a)
            host = urlparse(href).netloc
            if host and "simplybearings.co.uk" not in host:
                continue

            if PRODUCT_RE.search(href):
                # Skip if we already have it
                if href in self.known_urls:
                    continue
                yield scrapy.Request(href, callback=self.parse_product)
            elif "/shop/" in href:
                yield scrapy.Request(href, callback=self.parse_list)

    def parse_product(self, response):
        # Use BeautifulSoup for flexible text extraction
        soup = BeautifulSoup(response.text, "lxml")
        item = ProductItem()
        item["url"] = response.url

        # Title
        t = soup.find(["h1","h2","h3"])
        item["title"] = t.get_text(strip=True) if t else None

        # Free-text body for regex grabs
        body_text = soup.get_text("\n", strip=True)

        # SKU / Product Code
        m = re.search(r"(?:Product\s*Code|SKU)\s*[:\-]\s*([A-Z0-9\-\_\/\. ]+)", body_text, flags=re.I)
        item["sku"] = m.group(1).strip() if m else None

        # Brand / Quality
        m = re.search(r"Brand\s*/\s*Quality\s*[:\-]\s*([^\n]+)", body_text, flags=re.I)
        item["brand_quality"] = m.group(1).strip() if m else None

        # Price (GBP)
        m = re.search(r"£\s*([0-9]+(?:\.[0-9]{2})?)", body_text)
        item["price_gbp"] = m.group(1) if m else None

        # Availability
        m = re.search(r"(In Stock|Out of Stock|Backorder|Available)", body_text, flags=re.I)
        item["availability"] = m.group(1) if m else None

        # First decent paragraph as description
        desc = None
        for p in soup.find_all("p"):
            s = p.get_text(" ", strip=True)
            if s and len(s) > 40:
                desc = s
                break
        item["description"] = desc

        # Breadcrumbs
        crumbs = [a.get_text(strip=True) for a in soup.select("nav a, .breadcrumb a, .breadcrumbs a")]
        item["breadcrumbs"] = " > ".join(crumbs) if crumbs else None

        # Images
        imgs = [response.urljoin(img.get("src")) for img in soup.find_all("img") if img.get("src")]
        imgs = [i for i in imgs if "cdn" in i or "/shop/" in i]
        # dedupe order-preserving
        seen = {}
        for i in imgs:
            seen.setdefault(i, True)
        item["image_urls"] = "|".join(seen.keys()) if seen else None

        yield item
