# sb_scraper/spiders/categories.py
import re
from urllib.parse import urljoin

import scrapy
from scrapy import Request


class CategoriesSpider(scrapy.Spider):
    name = "categories"

    # Start from a few “hubs”. Add/remove if you like.
    start_urls = [
        "https://simplybearings.co.uk/shop/",
        "https://simplybearings.co.uk/shop/brands.php",
        "https://simplybearings.co.uk/shop/Bearings/c3/index.html",
        "https://simplybearings.co.uk/shop/All-Oil-Seals/c4747_5571/index.html",
    ]

    # This spider doesn’t need proxies; keep it light and fast.
    custom_settings = {
        "TELNETCONSOLE_ENABLED": False,
        "COOKIES_ENABLED": False,
        "LOG_LEVEL": "INFO",
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_DELAY": 1.0,
        "DOWNLOAD_TIMEOUT": 45,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.0,
        "AUTOTHROTTLE_MAX_DELAY": 20.0,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 2.0,
        "RETRY_TIMES": 5,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 522, 524, 408, 403, 429],

        # De-duplicate requests by URL without query string
        "DUPEFILTER_CLASS": "sb_scraper.dupe.CanonicalNoQueryDupeFilter",

        # Explicitly disable your proxy middleware for this spider
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy.downloadermiddlewares.offsite.OffsiteMiddleware": 500,
            "scrapy.downloadermiddlewares.httpauth.HttpAuthMiddleware": 700,
            "scrapy.downloadermiddlewares.downloadtimeout.DownloadTimeoutMiddleware": 350,
            "scrapy.downloadermiddlewares.defaultheaders.DefaultHeadersMiddleware": 540,
            "sb_scraper.middlewares.RandomUserAgentMiddleware": 400,
            "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": 500,
            "scrapy.downloadermiddlewares.retry.RetryMiddleware": 550,
            "scrapy.downloadermiddlewares.redirect.MetaRefreshMiddleware": 580,
            "scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware": 590,
            "scrapy.downloadermiddlewares.redirect.RedirectMiddleware": 600,
            # NOTE: no RotatingProxyMiddleware here
        },

        # No pipelines needed for categories export
        "ITEM_PIPELINES": {},
    }

    # Patterns that usually mean “not a category” or utility pages
    _SKIP_PATH_PATTERNS = re.compile(
        r"(about|contact|gdpr|privacy|terms|login|account|basket|search|newsletter)",
        re.I,
    )

    # Phrases that indicate the page has no products
    _EMPTY_TEXTS = (
        "sorry, no product found",
        "sorry, no products found",
        "no products found",
        "there are no products",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Item de-dupe: keep only first occurrence of each URL
        self.seen_item_urls: set[str] = set()

    def parse(self, response):
        # Extract all links on hub pages
        for a in response.css("a[href]"):
            name = (a.xpath("normalize-space(string(.))").get() or "").strip()
            href = urljoin(response.url, a.attrib.get("href", "").strip())

            # Keep it on-site
            if not href.startswith("https://simplybearings.co.uk/shop/"):
                continue

            # Quick skip for obvious non-category paths
            if self._SKIP_PATH_PATTERNS.search(href):
                continue

            # Don’t follow anchors or mailto/javascripts
            if href.startswith(("mailto:", "javascript:", "#")):
                continue

            # If name is blank, ignore (this fixes your empty ‘name’ rows)
            if not name:
                continue

            # Request the page to check whether it has products
            yield Request(
                href,
                callback=self.check_category_page,
                meta={"name": name, "source": response.url},
                dont_filter=False,
            )

    def check_category_page(self, response):
        name = response.meta["name"].strip()
        url = response.url
        source = response.meta.get("source", "")

        # Normalize page text to detect “no products”
        page_text = (response.xpath("normalize-space(//body)").get() or "").lower()

        if any(p in page_text for p in self._EMPTY_TEXTS):
            self.logger.debug(f"[skip empty] {url}")
            return

        # Optional: also check for a product grid/list existence (more robust)
        has_products = bool(
            response.css(".productlisting, .productgrid, .product_list, .productRow, .product-listing")
        )
        # If no common product containers are found AND no name in H1-ish tags, you can choose to skip.
        # We will keep the item if either products exist OR page didn’t show an explicit “no products” message.
        # if not has_products:
        #     return

        # De-duplicate items (same URL from different sources)
        if url in self.seen_item_urls:
            return
        self.seen_item_urls.add(url)

        yield {
            "name": name,
            "url": url,
            "source": source,
        }
