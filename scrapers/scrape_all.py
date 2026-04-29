"""
Reholstered.com — Master Scraper
Scrapes 50 holster brand websites and saves to Supabase
Run: python scrapers/scrape_all.py
"""

import os
import re
import sys
import json
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# Force unbuffered output so GitHub Actions shows logs in real time
print("Starting Reholstered scraper...", flush=True)

# ─── Supabase config (set in GitHub Secrets) ───────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ─── Carry type keyword mapping ─────────────────────────────────────────────
CARRY_KEYWORDS = {
    "aiwb": ["aiwb", "appendix"],
    "iwb": ["iwb", "inside waistband", "inside the waistband"],
    "owb": ["owb", "outside waistband", "outside the waistband", "paddle"],
    "duty": ["duty", "level ii", "level iii", "retention"],
    "shoulder": ["shoulder"],
    "ankle": ["ankle"],
    "chest": ["chest rig", "chest holster"],
    "offbody": ["off-body", "off body", "bag", "purse carry"],
}

def detect_carry(text):
    text = text.lower()
    for carry, keywords in CARRY_KEYWORDS.items():
        if any(k in text for k in keywords):
            return carry
    return "iwb"  # default

def detect_hand(text):
    text = text.lower()
    if "left" in text: return "left"
    if "ambi" in text: return "ambi"
    return "right"  # default

def clean_price(price_str):
    if not price_str:
        return None
    nums = re.findall(r"[\d]+\.?[\d]*", str(price_str).replace(",", ""))
    return float(nums[0]) if nums else None

def save_to_supabase(products):
    """Save scraped products to Supabase."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("⚠️  No Supabase credentials — saving to local JSON instead")
        with open("data/products.json", "w") as f:
            json.dump(products, f, indent=2)
        print(f"✅ Saved {len(products)} products to data/products.json", flush=True)
        return

    url = f"{SUPABASE_URL}/rest/v1/holsters"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

    # Upsert in batches of 100
    batch_size = 100
    total_saved = 0
    for i in range(0, len(products), batch_size):
        batch = products[i:i + batch_size]
        r = requests.post(url, headers=headers, json=batch, timeout=30)
        if r.status_code in (200, 201):
            total_saved += len(batch)
        else:
            print(f"❌ Supabase error: {r.status_code} — {r.text[:200]}", flush=True)

    print(f"✅ Saved {total_saved}/{len(products)} products to Supabase", flush=True)


# ═══════════════════════════════════════════════════════════════════════════
# SHOPIFY SCRAPER — works for ~30 of our 50 brands
# Most holster brands use Shopify, which exposes /products.json
# ═══════════════════════════════════════════════════════════════════════════

def scrape_shopify(brand_name, base_url, delay=1.5):
    """
    Scrape any Shopify store via /products.json endpoint.
    Returns list of product dicts.
    """
    products = []
    page = 1
    print(f"  Scraping {brand_name} (Shopify)...", flush=True)

    while True:
        url = f"{base_url}/products.json?limit=250&page={page}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code != 200:
                break
            data = r.json()
            items = data.get("products", [])
            if not items:
                break

            for item in items:
                title = item.get("title", "")
                handle = item.get("handle", "")
                product_type = item.get("product_type", "")
                tags = " ".join(item.get("tags", []))
                body = BeautifulSoup(item.get("body_html", ""), "html.parser").get_text(" ")
                combined_text = f"{title} {product_type} {tags} {body}"

                # Skip non-holster products
                if not any(kw in combined_text.lower() for kw in ["holster", "iwb", "owb", "aiwb", "carry"]):
                    continue

                # Get image
                images = item.get("images", [])
                image_url = images[0].get("src", "") if images else ""

                # Get price from first available variant
                variants = item.get("variants", [])
                price = None
                for v in variants:
                    if v.get("available", True):
                        price = clean_price(v.get("price"))
                        break
                if price is None and variants:
                    price = clean_price(variants[0].get("price"))

                products.append({
                    "brand": brand_name,
                    "name": title,
                    "price": price,
                    "image_url": image_url,
                    "product_url": f"{base_url}/products/{handle}",
                    "carry_type": detect_carry(combined_text),
                    "draw_hand": detect_hand(combined_text),
                    "source": "shopify",
                    "last_scraped": datetime.utcnow().isoformat(),
                })

            page += 1
            time.sleep(delay)

        except Exception as e:
            print(f"    ❌ Error on page {page}: {e}", flush=True)
            break

    print(f"    ✅ {brand_name}: {len(products)} holsters found", flush=True)
    return products


# ═══════════════════════════════════════════════════════════════════════════
# CUSTOM SCRAPERS — for non-Shopify brands
# ═══════════════════════════════════════════════════════════════════════════

def scrape_safariland():
    brand = "Safariland"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.safariland.com"
    urls = [
        "/products/holsters/duty-holsters/",
        "/products/holsters/concealment-holsters/",
        "/products/holsters/competition-holsters/",
    ]
    for path in urls:
        try:
            r = requests.get(base + path, headers=HEADERS, timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            cards = soup.select(".product-card, .product-tile, [data-product-id]")
            for card in cards:
                name_el = card.select_one(".product-name, .product-title, h2, h3")
                price_el = card.select_one(".price, .product-price, [class*='price']")
                img_el = card.select_one("img")
                link_el = card.select_one("a")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                price = clean_price(price_el.get_text() if price_el else None)
                image = img_el.get("src", img_el.get("data-src", "")) if img_el else ""
                link = base + link_el.get("href", "") if link_el else ""
                if not image.startswith("http"):
                    image = base + image if image else ""
                products.append({
                    "brand": brand, "name": name, "price": price,
                    "image_url": image, "product_url": link,
                    "carry_type": detect_carry(name),
                    "draw_hand": detect_hand(name),
                    "source": "custom",
                    "last_scraped": datetime.utcnow().isoformat(),
                })
            time.sleep(0.5)
        except Exception as e:
            print(f"    ❌ {path}: {e}", flush=True)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_blackhawk():
    brand = "Blackhawk"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.blackhawk.com"
    try:
        r = requests.get(f"{base}/holsters", headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".product-card, .product-item, [class*='product']")
        for card in cards:
            name_el = card.select_one("h2, h3, .product-name, .product-title")
            price_el = card.select_one(".price, [class*='price']")
            img_el = card.select_one("img")
            link_el = card.select_one("a")
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            if not name:
                continue
            price = clean_price(price_el.get_text() if price_el else None)
            image = img_el.get("src", img_el.get("data-src", "")) if img_el else ""
            link = link_el.get("href", "") if link_el else ""
            if link and not link.startswith("http"):
                link = base + link
            if image and not image.startswith("http"):
                image = base + image
            products.append({
                "brand": brand, "name": name, "price": price,
                "image_url": image, "product_url": link,
                "carry_type": detect_carry(name),
                "draw_hand": detect_hand(name),
                "source": "custom",
                "last_scraped": datetime.utcnow().isoformat(),
            })
    except Exception as e:
        print(f"    ❌ {e}", flush=True)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_galco():
    brand = "Galco"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.galcogunleather.com"
    categories = ["/product-listing/iwb-holsters/", "/product-listing/owb-holsters/",
                  "/product-listing/shoulder-holsters/", "/product-listing/ankle-holsters/"]
    for cat in categories:
        try:
            r = requests.get(base + cat, headers=HEADERS, timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            cards = soup.select(".productItem, .product-item, [class*='product']")
            for card in cards:
                name_el = card.select_one(".productName, h2, h3, .product-name")
                price_el = card.select_one(".productPrice, .price, [class*='price']")
                img_el = card.select_one("img")
                link_el = card.select_one("a")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                price = clean_price(price_el.get_text() if price_el else None)
                image = img_el.get("src", img_el.get("data-src", "")) if img_el else ""
                link = link_el.get("href", "") if link_el else ""
                if link and not link.startswith("http"):
                    link = base + link
                if image and not image.startswith("http"):
                    image = base + image
                products.append({
                    "brand": brand, "name": name, "price": price,
                    "image_url": image, "product_url": link,
                    "carry_type": detect_carry(cat + " " + name),
                    "draw_hand": detect_hand(name),
                    "source": "custom",
                    "last_scraped": datetime.utcnow().isoformat(),
                })
            time.sleep(0.5)
        except Exception as e:
            print(f"    ❌ {cat}: {e}", flush=True)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_desantis():
    brand = "DeSantis"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.desantisholster.com"
    try:
        r = requests.get(f"{base}/all-holsters/", headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".product, .woocommerce-LoopProduct, [class*='product-']")
        for card in cards:
            name_el = card.select_one("h2, h3, .woocommerce-loop-product__title")
            price_el = card.select_one(".price, .amount")
            img_el = card.select_one("img")
            link_el = card.select_one("a")
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            price = clean_price(price_el.get_text() if price_el else None)
            image = img_el.get("src", img_el.get("data-src", "")) if img_el else ""
            link = link_el.get("href", "") if link_el else ""
            products.append({
                "brand": brand, "name": name, "price": price,
                "image_url": image, "product_url": link,
                "carry_type": detect_carry(name),
                "draw_hand": detect_hand(name),
                "source": "custom",
                "last_scraped": datetime.utcnow().isoformat(),
            })
    except Exception as e:
        print(f"    ❌ {e}", flush=True)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_comptac():
    brand = "Comp-Tac"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.comp-tac.com"
    try:
        r = requests.get(f"{base}/holsters/", headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".product, [class*='product-item']")
        for card in cards:
            name_el = card.select_one("h2, h3, .product-title")
            price_el = card.select_one(".price, .amount")
            img_el = card.select_one("img")
            link_el = card.select_one("a")
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            price = clean_price(price_el.get_text() if price_el else None)
            image = img_el.get("src", "") if img_el else ""
            link = link_el.get("href", "") if link_el else ""
            products.append({
                "brand": brand, "name": name, "price": price,
                "image_url": image, "product_url": link,
                "carry_type": detect_carry(name),
                "draw_hand": detect_hand(name),
                "source": "custom",
                "last_scraped": datetime.utcnow().isoformat(),
            })
    except Exception as e:
        print(f"    ❌ {e}", flush=True)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_blade_tech():
    brand = "Blade-Tech"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.blade-tech.com"
    try:
        r = requests.get(f"{base}/holsters/", headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".product-item, .product, [class*='product']")
        for card in cards:
            name_el = card.select_one("h2, h3, .product-name")
            price_el = card.select_one(".price, [class*='price']")
            img_el = card.select_one("img")
            link_el = card.select_one("a")
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            price = clean_price(price_el.get_text() if price_el else None)
            image = img_el.get("src", img_el.get("data-src", "")) if img_el else ""
            link = link_el.get("href", "") if link_el else ""
            if link and not link.startswith("http"):
                link = base + link
            products.append({
                "brand": brand, "name": name, "price": price,
                "image_url": image, "product_url": link,
                "carry_type": detect_carry(name),
                "draw_hand": detect_hand(name),
                "source": "custom",
                "last_scraped": datetime.utcnow().isoformat(),
            })
    except Exception as e:
        print(f"    ❌ {e}", flush=True)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_gcode():
    brand = "G-Code Holsters"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.tacticalholsters.com"
    try:
        r = requests.get(f"{base}/holsters/", headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".product-item, .product, [class*='product']")
        for card in cards:
            name_el = card.select_one("h2, h3, .product-name")
            price_el = card.select_one(".price, [class*='price']")
            img_el = card.select_one("img")
            link_el = card.select_one("a")
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            price = clean_price(price_el.get_text() if price_el else None)
            image = img_el.get("src", "") if img_el else ""
            link = link_el.get("href", "") if link_el else ""
            if link and not link.startswith("http"):
                link = base + link
            products.append({
                "brand": brand, "name": name, "price": price,
                "image_url": image, "product_url": link,
                "carry_type": detect_carry(name),
                "draw_hand": detect_hand(name),
                "source": "custom",
                "last_scraped": datetime.utcnow().isoformat(),
            })
    except Exception as e:
        print(f"    ❌ {e}", flush=True)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_fobus():
    brand = "Fobus"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.fobusholster.com"
    try:
        r = requests.get(f"{base}/holsters/", headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".product, .woocommerce-LoopProduct, [class*='product']")
        for card in cards:
            name_el = card.select_one("h2, h3, .woocommerce-loop-product__title")
            price_el = card.select_one(".price, .amount")
            img_el = card.select_one("img")
            link_el = card.select_one("a")
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            price = clean_price(price_el.get_text() if price_el else None)
            image = img_el.get("src", "") if img_el else ""
            link = link_el.get("href", "") if link_el else ""
            products.append({
                "brand": brand, "name": name, "price": price,
                "image_url": image, "product_url": link,
                "carry_type": detect_carry(name),
                "draw_hand": detect_hand(name),
                "source": "custom",
                "last_scraped": datetime.utcnow().isoformat(),
            })
    except Exception as e:
        print(f"    ❌ {e}", flush=True)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_miltsparks():
    brand = "Milt Sparks"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.miltsparks.com"
    try:
        r = requests.get(f"{base}/holsters/", headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".product, .entry, [class*='product']")
        for card in cards:
            name_el = card.select_one("h2, h3, .entry-title")
            price_el = card.select_one(".price, .amount")
            img_el = card.select_one("img")
            link_el = card.select_one("a")
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            price = clean_price(price_el.get_text() if price_el else None)
            image = img_el.get("src", "") if img_el else ""
            link = link_el.get("href", "") if link_el else ""
            products.append({
                "brand": brand, "name": name, "price": price,
                "image_url": image, "product_url": link,
                "carry_type": detect_carry(name),
                "draw_hand": "right",
                "source": "custom",
                "last_scraped": datetime.utcnow().isoformat(),
            })
    except Exception as e:
        print(f"    ❌ {e}", flush=True)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_don_hume():
    brand = "Don Hume"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://donhume.com"
    try:
        r = requests.get(f"{base}/product-category/holsters/", headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".product, .woocommerce-LoopProduct, [class*=\'product\']")
        for card in cards:
            name_el = card.select_one("h2, h3, .woocommerce-loop-product__title")
            price_el = card.select_one(".price, .amount")
            img_el = card.select_one("img")
            link_el = card.select_one("a")
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            price = clean_price(price_el.get_text() if price_el else None)
            image = img_el.get("src", "") if img_el else ""
            link = link_el.get("href", "") if link_el else ""
            products.append({
                "brand": brand, "name": name, "price": price,
                "image_url": image, "product_url": link,
                "carry_type": detect_carry(name),
                "draw_hand": detect_hand(name),
                "source": "custom",
                "last_scraped": datetime.utcnow().isoformat(),
            })
    except Exception as e:
        print(f"    ❌ {e}", flush=True)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products



# ═══════════════════════════════════════════════════════════════════════════
# BRAND REGISTRY — all 50 brands
# ═══════════════════════════════════════════════════════════════════════════

SHOPIFY_BRANDS = [
    # ── ORIGINAL 42 BRANDS ─────────────────────────────────────────────────
    ("Vedder Holsters",         "https://www.vedderholsters.com"),
    ("We The People",           "https://wethepeopleholsters.com"),
    ("Alien Gear",              "https://aliengearholsters.com"),
    ("Bravo Concealment",       "https://www.bravoconcealment.com"),
    ("Tulster",                 "https://tulster.com"),
    ("Concealment Express",     "https://www.concealmentexpress.com"),
    ("Tier 1 Concealed",        "https://www.tier1concealed.com"),
    ("CrossBreed",              "https://www.crossbreedholsters.com"),
    ("StealthGear USA",         "https://stealthgearusa.com"),
    ("Versacarry",              "https://versacarry.com"),
    ("CYA Supply",              "https://cyasupply.com"),
    ("Crucial Concealment",     "https://crucialconcealment.com"),
    ("Sticky Holsters",         "https://stickyholsters.com"),
    ("N8 Tactical",             "https://n8tactical.com"),
    ("Harry's Holsters",        "https://harrysholsters.com"),
    ("Henry Holsters",          "https://henryholsters.com"),
    ("Clinger Holsters",        "https://clingerholsters.com"),
    ("ComfortTac",              "https://comforttac.com"),
    ("Rounded Gear",            "https://www.roundedgear.com"),
    ("Flashbang Holsters",      "https://www.flashbangstore.com"),
    ("ANR Design",              "https://anrkydexholsters.com"),
    ("Dark Star Gear",          "https://darkstargear.com"),
    ("Hidden Hybrid",           "https://www.hiddenhybridholsters.com"),
    ("LAG Tactical",            "https://lagtactical.com"),
    ("TXC Holsters",            "https://txcholsters.com"),
    ("Tenicor",                 "https://tenicor.com"),
    ("Keepers Concealment",     "https://keepersconcealment.com"),
    ("JM Custom Kydex",         "https://jmcustomkydex.com"),
    ("Werkz",                   "https://werkz.com"),
    ("Raven Concealment",       "https://rcsgear.com"),
    ("BlackPoint Tactical",     "https://blackpointtactical.com"),
    ("Mission First Tactical",  "https://missionfirsttactical.com"),
    ("Remora",                  "https://remoraholsterstore.com"),
    ("Uncle Mike's",            "https://unclemikes.com"),
    ("Tagua Gunleather",        "https://taguagunleather.com"),
    ("Bawidamann",              "https://bawidamann.com"),
    ("GunfightersINC",          "https://gunfightersinc.com"),
    ("NSR Tactical",            "https://nsrtactical.com"),
    ("Black Rhino",             "https://blackrhinoconcealment.com"),
    ("C&G Holsters",            "https://cg-holsters.com"),
    ("Dara Holsters",           "https://daraholsters.com"),
    ("Fury Carry Solutions",    "https://furycarry.com"),

    # ── NEW 50 BRANDS ──────────────────────────────────────────────────────
    ("Crossfire Elite",         "https://crossfireelite.com"),
    ("Stealth Operator",        "https://stealthoperatorholsters.com"),
    ("N82 Tactical",            "https://www.n82tactical.com"),
    ("Sneaky Pete Holsters",    "https://www.sneakypeteholsters.com"),
    ("Phalanx Defense",         "https://phalanxdefense.com"),
    ("Recover Tactical",        "https://recover-tactical.com"),
    ("Black Scorpion Gear",     "https://www.blackscorpiongear.com"),
    ("Haley Strategic",         "https://haleystrategic.com"),
    ("Grey Ghost Gear",         "https://www.greyghostgear.com"),
    ("Blue Force Gear",         "https://www.blueforcegear.com"),
    ("Hogue",                   "https://www.hogueinc.com"),
    ("LAS Concealment",         "https://lasconcealment.com"),
    ("GunGoddess",              "https://www.gungoddess.com"),
    ("Bulldog Cases",           "https://www.bulldogcases.com"),
    ("Techna Clip",             "https://www.technaclip.com"),
    ("UM Tactical",             "https://www.umtactical.com"),
    ("Just Holster It",         "https://justholsterit.com"),
    ("DME Holsters",            "https://dmeholsters.com"),
    ("Red River Tactical",      "https://rrtholsters.com"),
    ("Houdini Holsters",        "https://houdiniholsters.com"),
    ("Eclipse Holsters",        "https://eclipseholsters.com"),
    ("Grizzle Leather",         "https://rgrizzleleather.com"),
    ("Hilliker Holsters",       "https://hillikerholsters.com"),
    ("Wright Leather Works",    "https://wrightleatherworks.com"),
    ("Kirkpatrick Leather",     "https://kirkpatrickleather.com"),
    ("Don Hume",                "https://donhume.com"),
    ("PS Products",             "https://psproducts.com"),
    ("ComfortTac Premium",      "https://comforttacpremium.com"),
    ("KSG Armory",              "https://ksgarmory.com"),
    ("Talon Holsters",          "https://talonholsters.com"),
    ("Texas Holster Solutions", "https://texasholstersolutions.com"),
    ("Contact Concealment",     "https://contactconcealment.com"),
    ("ProTEQ Custom Gear",      "https://proteqgear.com"),
    ("Fist Holsters",           "https://www.fist-inc.com"),
    ("Looper Brand",            "https://looperbrand.com"),
    ("Kusiak Leather",          "https://kusiakleather.com"),
    ("Desantis Gunhide",        "https://www.desantisholster.com"),
    ("Crossfire Holsters",      "https://crossfireholsters.com"),
    ("Falco Holsters",          "https://www.falcoholsters.com"),
    ("Craft Holsters",          "https://www.craftholsters.com"),
    ("T-Rex Arms",              "https://trex-arms.com"),
    ("Phlster Holsters",        "https://phlsterholsters.com"),
    ("Comp-Tac Victory Gear",   "https://www.comp-tac.com"),
    ("Bianchi Leather",         "https://bianchileather.com"),
    ("El Paso Saddlery",        "https://epsaddlery.com"),
    ("Fury Tactical",           "https://furytactical.com"),
    ("Black Hills Leather",     "https://blackhillsleather.com"),
    ("Baker Leather",           "https://www.bakerleather.com"),
    ("High Noon Holsters",      "https://highnoonholsters.com"),
    ("K Rounds",                "https://kroundsholsters.com"),
    ("Cloak and Carry",         "https://cloakandcarry.com"),
    ("Gould & Goodrich",        "https://gouldusa.com"),
    ("Viridian Weapon Tech",    "https://viridianweapontech.com"),
    ("Elite Survival Systems",  "https://elitesurvival.com"),
    ("Covert Carry",            "https://covertcarry.com"),
    ("Hillman Holsters",        "https://hillmanholsters.com"),
    ("Tac-Pac",                 "https://tacpac.com"),
    ("KNG Tactical",            "https://kngtactical.com"),
]

CUSTOM_SCRAPERS = [
    scrape_safariland,    # #1 duty holster brand worldwide
    scrape_blackhawk,     # #2 duty/tactical brand
    scrape_galco,         # #3 leather holster brand
    scrape_desantis,      # #4 leather/kydex brand
    scrape_comptac,       # #5 competition/carry brand
    scrape_blade_tech,    # #6 competition holster brand
    scrape_gcode,         # #7 duty/tactical brand
    scrape_fobus,         # #8 polymer holster brand
    scrape_miltsparks,    # #9 premium leather brand
    scrape_don_hume,      # #10 classic leather brand
]


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    all_products = []
    errors = []

    print(f"\n{'='*60}", flush=True)
    print(f"  REHOLSTERED SCRAPER — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}", flush=True)
    print(f"{'='*60}\n", flush=True)

    # Shopify brands
    print("── SHOPIFY BRANDS ──────────────────────────────────────")
    for brand_name, base_url in SHOPIFY_BRANDS:
        try:
            products = scrape_shopify(brand_name, base_url)
            all_products.extend(products)
        except Exception as e:
            errors.append(f"{brand_name}: {e}")
            print(f"    ❌ FAILED: {e}", flush=True)
        time.sleep(0.5)

    # Custom scrapers
    print("\n── CUSTOM SCRAPERS ─────────────────────────────────────")
    for scraper_fn in CUSTOM_SCRAPERS:
        try:
            products = scraper_fn()
            all_products.extend(products)
        except Exception as e:
            errors.append(f"{scraper_fn.__name__}: {e}")
            print(f"    ❌ FAILED: {e}", flush=True)
        time.sleep(0.5)

    # Summary
    print(f"\n{'='*60}", flush=True)
    print(f"  TOTAL: {len(all_products)} holsters scraped", flush=True)
    if errors:
        print(f"  ERRORS ({len(errors)}):", flush=True)
        for err in errors:
            print(f"    • {err}", flush=True)
    print(f"{'='*60}\n", flush=True)

    # Save
    os.makedirs("data", exist_ok=True)
    save_to_supabase(all_products)

    return all_products


if __name__ == "__main__":
    main()
# Note: Additional brands appended below — total Shopify list now 100
SHOPIFY_BRANDS_EXTRA = [
    ("Cloak and Carry",         "https://cloakandcarry.com"),
    ("Gould & Goodrich",        "https://gouldusa.com"),
    ("Viridian Weapon Tech",    "https://viridianweapontech.com"),
    ("Elite Survival Systems",  "https://elitesurvival.com"),
    ("Safariland ALS",          "https://www.safarilandgroup.com"),
    ("Alien Gear Holsters CA",  "https://aliengearholsters.ca"),
    ("Covert Carry",            "https://covertcarry.com"),
    ("Hillman Holsters",        "https://hillmanholsters.com"),
]
SHOPIFY_BRANDS = SHOPIFY_BRANDS + SHOPIFY_BRANDS_EXTRA
