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


# ─── Gun model detection map ────────────────────────────────────────────────
GUN_MODELS = {
    # Glock — order matters, check specific models before generic
    "glock 43x": ["glock 43x", "g43x", "43x mos"],
    "glock 43":  [" glock 43 ", " g43 "],
    "glock 48":  ["glock 48", " g48 "],
    "glock 19x": ["glock 19x", "g19x"],
    "glock 19":  [" glock 19 ", " g19 ", "glock 19,", "glock 19/"],
    "glock 17":  [" glock 17 ", " g17 ", "glock 17,", "glock 17/"],
    "glock 26":  ["glock 26", " g26 "],
    "glock 27":  ["glock 27", " g27 "],
    "glock 22":  ["glock 22", " g22 "],
    "glock 23":  ["glock 23", " g23 "],
    "glock 20":  ["glock 20", " g20 "],
    "glock 21":  ["glock 21", " g21 "],
    "glock 29":  ["glock 29", " g29 "],
    "glock 30":  ["glock 30", " g30 "],
    "glock 34":  ["glock 34", " g34 "],
    "glock 41":  ["glock 41", " g41 "],
    "glock 42":  ["glock 42", " g42 "],
    "glock 45":  ["glock 45", " g45 "],
    # Sig Sauer
    "sig p365xl":      ["p365xl", "p365 xl", "p365-xl"],
    "sig p365x macro": ["p365x macro", "p365-x macro"],
    "sig p365":        [" p365 ", "p365,", "p365/"],
    "sig p320 xcarry": ["p320 x-carry", "p320 xcarry", "x-carry"],
    "sig p320 compact":["p320 compact", " p320c "],
    "sig p320 full":   ["p320 full", "p320 fs"],
    "sig p320":        [" p320 "],
    "sig p226":        [" p226 ", "p226,"],
    "sig p229":        [" p229 "],
    "sig p938":        [" p938 "],
    # Smith & Wesson
    "sw mp shield plus":["m&p shield plus", "shield plus", "mp shield plus"],
    "sw mp shield ez":  ["shield ez", "m&p ez"],
    "sw mp shield":     ["m&p shield ", "mp shield ", " shield "],
    "sw mp 2.0 compact":["m&p 2.0 compact", "m2.0 compact", "mp 2.0 compact", "m&p m2.0 compact"],
    "sw mp 2.0":        ["m&p 2.0", "m&p m2.0", "mp 2.0"],
    "sw equalizer":     ["equalizer"],
    "sw csx":           [" csx "],
    # Springfield
    "springfield hellcat pro":["hellcat pro"],
    "springfield hellcat rdp": ["hellcat rdp"],
    "springfield hellcat":     [" hellcat "],
    "springfield echelon":     [" echelon "],
    "springfield xdm elite":   ["xd-m elite", "xdm elite"],
    "springfield xds":         [" xd-s ", " xds "],
    "springfield xdm":         [" xd-m ", " xdm "],
    "springfield xd":          [" xd-9 ", " xd-40 ", " xd "],
    # H&K
    "hk vp9sk": ["vp9sk"],
    "hk vp9":   [" vp9 ", " vp9,"],
    "hk p30sk": ["p30sk"],
    "hk p30l":  [" p30l "],
    "hk p30":   [" p30 ", " p30,"],
    "hk usp compact": ["usp compact"],
    "hk usp":   [" usp "],
    # CZ
    "cz p10c": ["p-10 c", " p10c", "p-10c"],
    "cz p10f": ["p-10 f", " p10f", "p-10f"],
    "cz p10s": ["p-10 s", " p10s"],
    "cz p07":  [" p-07 ", " p07 "],
    "cz p09":  [" p-09 ", " p09 "],
    # Walther
    "walther pdp compact": ["pdp compact"],
    "walther pdp":  [" pdp full", " pdp "],
    "walther ppq":  [" ppq "],
    "walther pps":  [" pps "],
    # Ruger
    "ruger max9":     ["max-9", " max9"],
    "ruger security9":["security-9", "security9"],
    "ruger lcp max":  ["lcp max"],
    "ruger lcp2":     ["lcp ii", " lcp2"],
    "ruger lcp":      [" lcp "],
    # Taurus
    "taurus g3c": [" g3c "],
    "taurus g3":  ["taurus g3 "],
    "taurus gx4": [" gx4 "],
    "taurus g2c": [" g2c "],
    # Canik
    "canik tp9sf": ["tp9sf", "tp9 sf"],
    "canik mete":  ["mete sf", "mete sft", "mete sfx"],
    # FN
    "fn 509 compact": ["509 compact", "509c "],
    "fn 509 tactical":["509 tactical"],
    "fn 509":  [" fn509 ", " 509 "],
    # Kimber
    "kimber micro9": ["micro 9", "micro9"],
    # Beretta
    "beretta 92fs": ["92fs", "92 fs"],
    "beretta apx":  [" apx "],
    "beretta px4":  [" px4 "],
    # 1911
    "1911 5 inch": ["1911 5 inch", "1911 government", "1911 5\""],
    "1911 4 inch": ["1911 4 inch", "1911 commander", "1911 4\""],
    "1911 3 inch": ["1911 3 inch", "1911 officer", "1911 3\""],
    "1911":        [" 1911 "],
}

def detect_gun_model(text):
    """Detect specific gun model from product text."""
    text = " " + text.lower() + " "
    for model, keywords in GUN_MODELS.items():
        if any(k in text for k in keywords):
            return model
    return None

# ─── Carry type keyword mapping ─────────────────────────────────────────────
CARRY_KEYWORDS = {
    "aiwb": ["aiwb", "appendix"],
    "iwb": ["iwb", "inside waistband", "inside the waistband", "tuckable", "tuck"],
    "owb": ["owb", "outside waistband", "outside the waistband", "paddle", "belt slide", "pancake"],
    "duty": ["duty", "level ii", "level iii", "retention", "als", "sls"],
    "shoulder": ["shoulder"],
    "ankle": ["ankle"],
    "chest": ["chest rig", "chest holster"],
    "offbody": ["off-body", "off body", "purse carry"],
}

# ─── Weapon light keyword mapping ────────────────────────────────────────────
LIGHT_KEYWORDS = {
    # Streamlight TLR series
    "tlr-1":    ["tlr-1 hl", "tlr1 hl", "tlr-1hl", "tlr1hl", "tlr 1 hl", "tlr-1s", "tlr1s", " tlr-1 ", " tlr1 ", "tlr 1 "],
    "tlr-7":    ["tlr-7a", "tlr7a", "tlr-7 a", "tlr 7a", " tlr-7 ", " tlr7 ", "tlr 7 "],
    "tlr-7-sub":["tlr-7 sub", "tlr7 sub", "tlr-7sub"],
    "tlr-8":    ["tlr-8a", "tlr8a", "tlr-8 a", "tlr 8a", " tlr-8 ", " tlr8 ", "tlr 8 "],
    "tlr-9":    [" tlr-9 ", " tlr9 ", "tlr 9 "],
    "tlr-10":   [" tlr-10", " tlr10", "tlr 10"],
    # SureFire
    "sf-x300u-a":["x300u-a", "x300u a", "x300ua"],
    "sf-x300u-b":["x300u-b", "x300u b", "x300ub"],
    "sf-x300":  [" x300 ", "x300u"],
    "sf-xc1":   [" xc1 ", "xc-1"],
    "sf-xc2":   [" xc2 ", "xc-2"],
    # Olight
    "olight-pl-mini2": ["pl-mini 2", "pl mini 2", "pl-mini2", "valkyrie mini"],
    "olight-pl-pro":   ["pl-pro", "pl pro", "valkyrie pro"],
    "olight-baldr-mini":["baldr mini", "baldrmini"],
    "olight-baldr-pro": ["baldr pro", "baldrpro"],
    "olight-baldr-s":   ["baldr s ","baldr-s"],
    # Nightstick
    "ns-twm-30": ["twm-30", "twm30"],
    "ns-twm-852":["twm-852", "twm852"],
    # Inforce
    "inforce-apl":["inforce apl", " apl ", "aplc"],
    # Cloud Defensive
    "cd-rein":  ["rein micro", "rein 2", "cd rein"],
    # Crimson Trace
    "ct-rail-master":["rail master", "cmr-207", "cmr207", "cmr-208", "cmr208"],
    # Generic
    "any":      ["weapon light", "wml", "light bearing", "w/ light", "with light", "w/light",
                 "light-bearing", "streamlight", "surefire", "olight"],
}

def detect_carry(text):
    text = text.lower()
    # Check AIWB first (more specific than IWB)
    if any(k in text for k in CARRY_KEYWORDS["aiwb"]):
        return "aiwb"
    for carry, keywords in CARRY_KEYWORDS.items():
        if carry == "aiwb":
            continue
        if any(k in text for k in keywords):
            return carry
    return "iwb"  # default

def detect_hand(text):
    text = text.lower()
    if "left hand" in text or "left-hand" in text or "lh " in text: return "left"
    if "ambi" in text: return "ambi"
    return "right"  # default

# ─── Optic cut detection ─────────────────────────────────────────────────────
OPTIC_KEYWORDS = {
    # Trijicon
    "rmr":          ["trijicon rmr", " rmr ", "rmr type 2", "rmrcc"],
    "sro":          ["trijicon sro", " sro "],
    "rmrcc":        ["rmrcc", "rmr cc"],
    # Holosun
    "holosun-507c": ["507c", "hs507c"],
    "holosun-508t": ["508t", "hs508t"],
    "holosun-509t": ["509t", "hs509t"],
    "holosun-407c": ["407c", "hs407c"],
    "holosun-510c": ["510c", "hs510c"],
    "holosun-any":  ["holosun"],
    # Shield
    "shield-sms":   ["shield sms", " sms "],
    "shield-rmsc":  ["shield rmsc", "rmsc", "shield rms"],
    # Sig
    "sig-romeo-zero":  ["romeo zero", "romeozero"],
    "sig-romeo1-pro":  ["romeo1 pro", "romeo 1 pro"],
    # Leupold
    "leupold-dpp": ["deltapoint pro", "delta point pro", " dpp "],
    # Vortex
    "vortex-venom":  ["vortex venom", " venom "],
    "vortex-razor":  ["vortex razor", " razor "],
    # EOTech
    "eotech-eflx":  ["eflx", "eotech eflx"],
    # Burris
    "burris-fastfire": ["fastfire", "fast fire"],
    # Crimson Trace
    "ct-cts1550":   ["cts-1550", "cts1550"],
    # Generic
    "any":          ["optic cut", "optic ready", "mos ", " or ", "rmr cut",
                     "red dot cut", "optic compatible", "suppressor height"],
}

def detect_optic(text):
    """Detect optic cut compatibility from product text."""
    text = " " + text.lower() + " "
    for optic_key, keywords in OPTIC_KEYWORDS.items():
        if any(k in text for k in keywords):
            return optic_key
    return None
    
    def detect_light(text):
    """Detect weapon light compatibility from product text."""
    text = " " + text.lower() + " "
    for light_key, keywords in LIGHT_KEYWORDS.items():
        if any(k in text for k in keywords):
            return light_key
    return None  # no light detected

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
        r = requests.post(url + "?on_conflict=product_url", headers=headers, json=batch, timeout=30)
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
            r = requests.get(url, headers=HEADERS, timeout=8)
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
                    "light": detect_light(combined_text),
                    "gun_model": detect_gun_model(combined_text),
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
            r = requests.get(base + path, headers=HEADERS, timeout=8)
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
                    "light": detect_light(name),
                    "gun_model": detect_gun_model(name),
                    "source": "custom",
                    "last_scraped": datetime.utcnow().isoformat(),
                })
            time.sleep(0.3)
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
        r = requests.get(f"{base}/holsters", headers=HEADERS, timeout=8)
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
            r = requests.get(base + cat, headers=HEADERS, timeout=8)
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
            time.sleep(0.3)
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
        r = requests.get(f"{base}/all-holsters/", headers=HEADERS, timeout=8)
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
        r = requests.get(f"{base}/holsters/", headers=HEADERS, timeout=8)
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
        r = requests.get(f"{base}/holsters/", headers=HEADERS, timeout=8)
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
        r = requests.get(f"{base}/holsters/", headers=HEADERS, timeout=8)
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
        r = requests.get(f"{base}/holsters/", headers=HEADERS, timeout=8)
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
        r = requests.get(f"{base}/holsters/", headers=HEADERS, timeout=8)
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
        r = requests.get(f"{base}/product-category/holsters/", headers=HEADERS, timeout=8)
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



def scrape_vedder():
    """Vedder uses BigCommerce — scrape their holster category pages."""
    brand = "Vedder Holsters"
    products = []
    print(f"  Scraping {brand} (BigCommerce)...", flush=True)
    categories = [
        "/holsters/iwb/",
        "/holsters/owb/",
        "/holsters/appendix/",
    ]
    for cat in categories:
        try:
            r = requests.get(base + cat, headers=HEADERS, timeout=8)
            soup = BeautifulSoup(r.text, "html.parser")
            cards = soup.select(".product, .productCard, [class*='product-card'], [class*='productCard']")
            for card in cards:
                name_el = card.select_one("h2, h3, h4, .product-title, .productTitle, [class*='title']")
                price_el = card.select_one(".price, .productPrice, [class*='price']")
                img_el = card.select_one("img")
                link_el = card.select_one("a")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name or len(name) < 3:
                    continue
                price = clean_price(price_el.get_text() if price_el else None)
                image = img_el.get("src", img_el.get("data-src", "")) if img_el else ""
                link = link_el.get("href", "") if link_el else ""
                if link and not link.startswith("http"):
                    link = base + link
                if image and not image.startswith("http"):
                    image = base + image
                combined = f"{name} {cat}"
                products.append({
                    "brand": brand, "name": name, "price": price,
                    "image_url": image, "product_url": link,
                    "carry_type": detect_carry(combined),
                    "draw_hand": detect_hand(name),
                    "light": detect_light(name),
                    "gun_model": detect_gun_model(name),
                    "source": "custom",
                    "last_scraped": datetime.utcnow().isoformat(),
                })
            time.sleep(0.5)
        except Exception as e:
            print(f"    ❌ Vedder {cat}: {e}", flush=True)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_crossbreed():
    """CrossBreed — scrape their holster pages."""
    brand = "CrossBreed Holsters"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    try:
        r = requests.get(f"{base}/holsters/", headers=HEADERS, timeout=8)
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".product-item, .product, [class*='product']")
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
                "light": detect_light(name),
                "gun_model": detect_gun_model(name),
                "source": "custom",
                "last_scraped": datetime.utcnow().isoformat(),
            })
    except Exception as e:
        print(f"    ❌ CrossBreed: {e}", flush=True)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_stealthgear():
    """StealthGear USA — scrape their holster pages."""
    brand = "StealthGear USA"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    try:
        r = requests.get(f"{base}/collections/holsters", headers=HEADERS, timeout=8)
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".product-item, .grid__item, [class*='product']")
        for card in cards:
            name_el = card.select_one("h2, h3, .product-item__title, .product__title")
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
                image = "https:" + image if image.startswith("//") else base + image
            products.append({
                "brand": brand, "name": name, "price": price,
                "image_url": image, "product_url": link,
                "carry_type": detect_carry(name),
                "draw_hand": detect_hand(name),
                "light": detect_light(name),
                "gun_model": detect_gun_model(name),
                "source": "custom",
                "last_scraped": datetime.utcnow().isoformat(),
            })
    except Exception as e:
        print(f"    ❌ StealthGear: {e}", flush=True)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_tulster():
    """Tulster uses BigCommerce — scrape category pages."""
    brand = "Tulster"
    products = []
    print(f"  Scraping {brand} (BigCommerce)...", flush=True)
    base = "https://tulster.com"
    categories = [
        ("/inside-the-waistband-holster/profile-series/", "iwb"),
        ("/inside-the-waistband-holster/oath-series/", "iwb"),
        ("/inside-the-waistband-holster/profile-plus-series/", "iwb"),
        ("/outside-the-waistband-holster/", "owb"),
        ("/appendix-carry/", "aiwb"),
    ]
    seen = set()
    for path, carry in categories:
        try:
            r = requests.get(base + path, headers=HEADERS, timeout=8)
            soup = BeautifulSoup(r.text, "html.parser")
            # BigCommerce product cards
            cards = soup.select(".productCard, .product-item, [class*='productCard'], [data-product-id]")
            if not cards:
                # Try generic selectors
                cards = soup.select("article, .product, li.product")
            for card in cards:
                name_el = card.select_one("h2, h3, h4, [class*='title'], [class*='name']")
                price_el = card.select_one("[class*='price'], .price")
                img_el = card.select_one("img")
                link_el = card.select_one("a[href*='/']")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name or len(name) < 5 or name in seen:
                    continue
                seen.add(name)
                price = clean_price(price_el.get_text() if price_el else None)
                image = img_el.get("src", img_el.get("data-src", "")) if img_el else ""
                link = link_el.get("href", "") if link_el else ""
                if link and not link.startswith("http"):
                    link = base + link
                if image and not image.startswith("http"):
                    image = "https:" + image if image.startswith("//") else base + image
                products.append({
                    "brand": brand, "name": name, "price": price,
                    "image_url": image, "product_url": link or base + path,
                    "carry_type": carry,
                    "draw_hand": detect_hand(name),
                    "light": detect_light(name),
                    "gun_model": detect_gun_model(name),
                    "source": "custom",
                    "last_scraped": datetime.utcnow().isoformat(),
                })
            time.sleep(0.5)
        except Exception as e:
            print(f"    ❌ Tulster {path}: {e}", flush=True)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_vedder():
    """Vedder uses BigCommerce."""
    brand = "Vedder Holsters"
    products = []
    print(f"  Scraping {brand} (BigCommerce)...", flush=True)
    base = "https://www.vedderholsters.com"
    categories = [
        ("/holsters/iwb/", "iwb"),
        ("/holsters/owb/", "owb"),
        ("/holsters/appendix/", "aiwb"),
        ("/holsters/pocket/", "offbody"),
    ]
    seen = set()
    for path, carry in categories:
        try:
            r = requests.get(base + path, headers=HEADERS, timeout=8)
            soup = BeautifulSoup(r.text, "html.parser")
            cards = soup.select(".productCard, .product-item, article, [class*='product']")
            for card in cards:
                name_el = card.select_one("h2, h3, h4, [class*='title'], [class*='name']")
                price_el = card.select_one("[class*='price'], .price")
                img_el = card.select_one("img")
                link_el = card.select_one("a[href*='/']")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name or len(name) < 5 or name in seen:
                    continue
                seen.add(name)
                price = clean_price(price_el.get_text() if price_el else None)
                image = img_el.get("src", img_el.get("data-src", "")) if img_el else ""
                link = link_el.get("href", "") if link_el else ""
                if link and not link.startswith("http"):
                    link = base + link
                if image and not image.startswith("http"):
                    image = "https:" + image if image.startswith("//") else base + image
                products.append({
                    "brand": brand, "name": name, "price": price,
                    "image_url": image, "product_url": link or base + path,
                    "carry_type": carry,
                    "draw_hand": detect_hand(name),
                    "light": detect_light(name),
                    "gun_model": detect_gun_model(name),
                    "source": "custom",
                    "last_scraped": datetime.utcnow().isoformat(),
                })
            time.sleep(0.5)
        except Exception as e:
            print(f"    ❌ Vedder {path}: {e}", flush=True)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_stealthgear():
    """StealthGear USA."""
    brand = "StealthGear USA"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://stealthgearusa.com"
    urls = [
        ("/collections/iwb-holsters", "iwb"),
        ("/collections/owb-holsters", "owb"),
        ("/collections/appendix-holsters", "aiwb"),
    ]
    seen = set()
    for path, carry in urls:
        try:
            r = requests.get(base + path, headers=HEADERS, timeout=8)
            soup = BeautifulSoup(r.text, "html.parser")
            cards = soup.select(".product-item, .grid__item, [class*='product-card']")
            for card in cards:
                name_el = card.select_one("h2, h3, .product-item__title, .product__title, [class*='title']")
                price_el = card.select_one(".price, [class*='price']")
                img_el = card.select_one("img")
                link_el = card.select_one("a[href*='/products/']")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name or name in seen:
                    continue
                seen.add(name)
                price = clean_price(price_el.get_text() if price_el else None)
                image = img_el.get("src", img_el.get("data-src", "")) if img_el else ""
                link = link_el.get("href", "") if link_el else ""
                if link and not link.startswith("http"):
                    link = base + link
                if image and not image.startswith("http"):
                    image = "https:" + image if image.startswith("//") else ""
                products.append({
                    "brand": brand, "name": name, "price": price,
                    "image_url": image, "product_url": link or base + path,
                    "carry_type": carry,
                    "draw_hand": detect_hand(name),
                    "light": detect_light(name),
                    "gun_model": detect_gun_model(name),
                    "source": "custom",
                    "last_scraped": datetime.utcnow().isoformat(),
                })
            time.sleep(0.5)
        except Exception as e:
            print(f"    ❌ StealthGear {path}: {e}", flush=True)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_crossbreed():
    """CrossBreed Holsters."""
    brand = "CrossBreed Holsters"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.crossbreedholsters.com"
    urls = [
        ("/holsters/iwb-holsters/", "iwb"),
        ("/holsters/owb-holsters/", "owb"),
        ("/holsters/appendix-carry/", "aiwb"),
    ]
    seen = set()
    for path, carry in urls:
        try:
            r = requests.get(base + path, headers=HEADERS, timeout=8)
            soup = BeautifulSoup(r.text, "html.parser")
            cards = soup.select(".product-item, .product, [class*='product-card'], article")
            for card in cards:
                name_el = card.select_one("h2, h3, h4, [class*='title'], [class*='name']")
                price_el = card.select_one("[class*='price'], .price")
                img_el = card.select_one("img")
                link_el = card.select_one("a[href*='/']")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name or len(name) < 5 or name in seen:
                    continue
                seen.add(name)
                price = clean_price(price_el.get_text() if price_el else None)
                image = img_el.get("src", img_el.get("data-src", "")) if img_el else ""
                link = link_el.get("href", "") if link_el else ""
                if link and not link.startswith("http"):
                    link = base + link
                if image and not image.startswith("http"):
                    image = "https:" + image if image.startswith("//") else base + image
                products.append({
                    "brand": brand, "name": name, "price": price,
                    "image_url": image, "product_url": link or base + path,
                    "carry_type": carry,
                    "draw_hand": detect_hand(name),
                    "light": detect_light(name),
                    "gun_model": detect_gun_model(name),
                    "source": "custom",
                    "last_scraped": datetime.utcnow().isoformat(),
                })
            time.sleep(0.5)
        except Exception as e:
            print(f"    ❌ CrossBreed {path}: {e}", flush=True)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_raven_concealment():
    """Raven Concealment Systems."""
    brand = "Raven Concealment"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://rcsgear.com"
    urls = [("/collections/holsters", "iwb")]
    seen = set()
    for path, carry in urls:
        try:
            r = requests.get(base + path, headers=HEADERS, timeout=8)
            soup = BeautifulSoup(r.text, "html.parser")
            cards = soup.select(".product-item, .grid-item, [class*='product']")
            for card in cards:
                name_el = card.select_one("h2, h3, [class*='title']")
                price_el = card.select_one("[class*='price']")
                img_el = card.select_one("img")
                link_el = card.select_one("a[href*='/products/']")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name or name in seen:
                    continue
                seen.add(name)
                price = clean_price(price_el.get_text() if price_el else None)
                image = img_el.get("src", img_el.get("data-src", "")) if img_el else ""
                link = link_el.get("href", "") if link_el else ""
                if link and not link.startswith("http"):
                    link = base + link
                if image and not image.startswith("http"):
                    image = "https:" + image if image.startswith("//") else ""
                products.append({
                    "brand": brand, "name": name, "price": price,
                    "image_url": image, "product_url": link or base + path,
                    "carry_type": detect_carry(name),
                    "draw_hand": detect_hand(name),
                    "light": detect_light(name),
                    "gun_model": detect_gun_model(name),
                    "source": "custom",
                    "last_scraped": datetime.utcnow().isoformat(),
                })
            time.sleep(0.5)
        except Exception as e:
            print(f"    ❌ Raven {path}: {e}", flush=True)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_versacarry():
    """Versacarry."""
    brand = "Versacarry"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://versacarry.com"
    try:
        r = requests.get(f"{base}/collections/holsters", headers=HEADERS, timeout=8)
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".product-item, [class*='product-card'], .grid__item")
        seen = set()
        for card in cards:
            name_el = card.select_one("h2, h3, [class*='title']")
            price_el = card.select_one("[class*='price']")
            img_el = card.select_one("img")
            link_el = card.select_one("a[href*='/products/']")
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            if not name or name in seen:
                continue
            seen.add(name)
            price = clean_price(price_el.get_text() if price_el else None)
            image = img_el.get("src", img_el.get("data-src", "")) if img_el else ""
            link = link_el.get("href", "") if link_el else ""
            if link and not link.startswith("http"):
                link = base + link
            if image and not image.startswith("http"):
                image = "https:" + image if image.startswith("//") else ""
            products.append({
                "brand": brand, "name": name, "price": price,
                "image_url": image, "product_url": link,
                "carry_type": detect_carry(name),
                "draw_hand": detect_hand(name),
                "light": detect_light(name),
                "gun_model": detect_gun_model(name),
                "source": "custom",
                "last_scraped": datetime.utcnow().isoformat(),
            })
    except Exception as e:
        print(f"    ❌ Versacarry: {e}", flush=True)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


# ═══════════════════════════════════════════════════════════════════════════
# BRAND REGISTRY — all 50 brands
# ═══════════════════════════════════════════════════════════════════════════

SHOPIFY_BRANDS = [
    # ── ORIGINAL 42 BRANDS ─────────────────────────────────────────────────
    ("We The People",           "https://wethepeopleholsters.com"),
    ("Alien Gear",              "https://aliengearholsters.com"),
    ("Bravo Concealment",       "https://www.bravoconcealment.com"),
    ("Concealment Express",     "https://www.concealmentexpress.com"),
    ("Tier 1 Concealed",        "https://www.tier1concealed.com"),
    ("CYA Supply",              "https://cyasupply.com"),
    ("Clinger Holsters",        "https://clingerholsters.com"),
    ("Rounded Gear",            "https://www.roundedgear.com"),
    ("Hidden Hybrid",           "https://www.hiddenhybridholsters.com"),
    ("LAG Tactical",            "https://lagtactical.com"),
    ("TXC Holsters",            "https://txcholsters.com"),
    ("Tenicor",                 "https://tenicor.com"),
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
    scrape_vedder,        # BigCommerce custom
    scrape_crossbreed,    # Custom
    scrape_stealthgear,   # Custom
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
        time.sleep(0.3)

    # Custom scrapers
    print("\n── CUSTOM SCRAPERS ─────────────────────────────────────")
    for scraper_fn in CUSTOM_SCRAPERS:
        try:
            products = scraper_fn()
            all_products.extend(products)
        except Exception as e:
            errors.append(f"{scraper_fn.__name__}: {e}")
            print(f"    ❌ FAILED: {e}", flush=True)
        time.sleep(0.3)

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
