import urllib.request, gzip, xml.etree.ElementTree as ET

SITEMAP_INDEX = "https://simplybearings.co.uk/shop/sitemapindex.xml"

def fetch_bytes(url):
    with urllib.request.urlopen(url) as r:
        data = r.read()
    return gzip.decompress(data) if url.endswith(".gz") else data

def parse_xml(data):
    return ET.fromstring(data)

def text(e): return e.text if e is not None else ""

def main():
    idx = parse_xml(fetch_bytes(SITEMAP_INDEX))
    # grab all sitemap <loc> entries
    sitemap_urls = [text(loc)
                    for loc in idx.findall(".//{*}sitemap/{*}loc")]
    # look only at product sitemaps
    product_sitemaps = [u for u in sitemap_urls if "sitemapproducts" in u.lower()]

    total = 0
    for sm in product_sitemaps:
        root = parse_xml(fetch_bytes(sm))
        locs = [text(loc) for loc in root.findall(".//{*}url/{*}loc")]
        count = sum(1 for u in locs if "/product_info.html" in (u or ""))
        print(f"{sm} -> {count}")
        total += count

    print("\nTOTAL product pages:", total)

if __name__ == "__main__":
    main()
