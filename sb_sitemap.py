# import re
# import json
# from datetime import datetime, timezone

# import scrapy
# from scrapy.spiders import SitemapSpider


# class SBSitemapOKSpider(SitemapSpider):
#     name = "sb_sitemap_ok"
#     allowed_domains = ["simplybearings.co.uk"]
#     sitemap_urls = ["https://simplybearings.co.uk/shop/sitemapindex.xml"]

#     # only follow product pages
#     sitemap_rules = [
#         (r"/product_info\.html$", "parse_product"),
#     ]

#     custom_settings = {
#         # keep encoding safe for CSV/JSON
#         "FEED_EXPORT_ENCODING": "utf-8",
#         # your throttling etc. can live in settings.py, this is just here for clarity
#         # "ROBOTSTXT_OBEY": True,
#     }

#     def parse_product(self, response):
#         # --- basics
#         product_url = response.url
#         title = response.css("h1::text").get() or response.css("title::text").get()

#         # --- breadcrumbs
#         breadcrumbs = [
#             " ".join(t.split()) for t in response.css(".breadcrumb a::text, nav.breadcrumbs a::text").getall()
#         ]

#         # --- price & currency
#         # try structured data first
#         price = response.css('[itemprop="price"]::attr(content), meta[itemprop="price"]::attr(content)').get()
#         if not price:
#             price_text = "".join(response.css(".price, .ProductPrice, .our_price_display::text").getall())
#             price_text = price_text.strip()
#             m = re.search(r"([\d\.,]+)", price_text)
#             price = m.group(1) if m else None

#         currency = (
#             response.css('[itemprop="priceCurrency"]::attr(content)').get()
#             or ("GBP" if "£" in response.text or "GBP" in response.text else None)
#         )

#         # --- attributes table (key/value)
#         attributes = {}
#         for row in response.css("table tr"):
#             key = " ".join(row.css("th, td:first-child::text").getall()).strip()
#             val = " ".join(row.css("td:last-child::text").getall()).strip()
#             if key and val and len(key) < 80 and len(val) < 500:
#                 attributes[key] = val

#         # --- dimensions (try to pull a 'Dimensions' block; fall back to size-looking rows)
#         dimensions = {}
#         # heuristic: pick rows whose header mentions common dimension words
#         for k, v in list(attributes.items()):
#             if re.search(r"(dimension|size|length|width|height|depth|bore|od|id|thick|mm|cm|in)", k, re.I):
#                 dimensions[k] = v

#         # --- variants (select/options)
#         variants = []
#         for sel in response.css("select"):
#             name = sel.attrib.get("name") or sel.attrib.get("id")
#             options = [o.css("::text").get(default="").strip() for o in sel.css("option") if o.attrib.get("value")]
#             if name and options:
#                 variants.append({"name": name, "options": [o for o in options if o]})

#         # --- images (absolute URLs)
#         images = [
#             response.urljoin(u)
#             for u in set(response.css('img::attr(src), link[rel="preload"][as="image"]::attr(href)').getall())
#             if u and not u.lower().endswith((".svg",))
#         ]

#         # --- documents (e.g., PDFs, datasheets)
#         documents = [
#             response.urljoin(h)
#             for h in response.css('a::attr(href)').getall()
#             if h and h.lower().endswith((".pdf", ".doc", ".docx"))
#         ]

#         # --- last seen
#         last_seen_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

#         yield {
#             "product_url": product_url,
#             "breadcrumbs": breadcrumbs,
#             "title": title,
#             "price": _to_number(price),
#             "currency": currency,
#             "attributes": attributes,
#             "dimensions": dimensions,
#             "variants": variants,
#             "images": images,
#             "documents": documents,
#             "last_seen_at": last_seen_at,
#         }


# def _to_number(s):
#     if not s:
#         return None
#     # normalize "1,234.56" or "1.234,56"
#     s = s.strip().replace(" ", "")
#     if s.count(",") == 1 and s.count(".") == 0:
#         s = s.replace(",", ".")
#     s = s.replace(",", "")
#     try:
#         return float(s)
#     except ValueError:
#         return None
# def __init__(self, urls_file=None, *args, **kwargs):
#     super().__init__(*args, **kwargs)
#     self.urls_file = urls_file

# def start_requests(self):
#     if self.urls_file:
#         with open(self.urls_file, "r", encoding="utf-8") as f:
#             for line in f:
#                 url = line.strip()
#                 if url:
#                     yield scrapy.Request(url, callback=self.parse_product)
#     else:
#         yield from super().start_requests()
import scrapy

class ArrowSpider(scrapy.Spider):
    name = "arrow_products"

    def start_requests(self):
        # Load URLs from the file
        with open("out/site_product_urls.txt", "r", encoding="utf-8") as f:
            urls = [line.strip() for line in f if line.strip()]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # Example product fields
        yield {
            "url": response.url,
            "title": response.css("h1.product_title::text").get(),
            "sku": response.css("span.sku::text").get(),
            "price": response.css("span.woocommerce-Price-amount bdi::text").get(),
            "description": response.css("div.woocommerce-Tabs-panel--description p::text").getall(),
        }
