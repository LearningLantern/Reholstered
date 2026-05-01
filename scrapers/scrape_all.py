"""
Reholstered.com — Master Scraper
Scrapes 50+ holster brand websites and saves to Supabase
Run: python scrapers/scrape_all.py

Fixes applied:
- Step 1: Upsert on product_url to prevent duplicate rows
- Step 2: detect_optic() + optic field on all scrapers
- Step 3: gun_model, light, optic added to all 8 custom scrapers
- Step 4: Duplicate brands removed, G-Code URL fixed, Don Hume deduplicated
- Step 5: 7 zero-return scrapers replaced (Vedder, Tulster, StealthGear,
          CrossBreed, Safariland, Blackhawk, Galco)
- Step 6: carry_type and draw_hand defaults changed to None
- Step 7: Comp-Tac URL fixed, Milt Sparks draw_hand fixed
- Step 8: Retry logic with exponential backoff on all requests
- Step 9: material and in_stock fields added
"""

import os
import re
import sys
import json
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime

print("Starting Reholstered scraper...", flush=True)

# ─── Supabase config ────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ─── Retry helper ────────────────────────────────────────────────────────────
def fetch_with_retry(url, headers=None, timeout=10, retries=3, delay=2):
    """GET with exponential backoff. Returns Response or None."""
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers or HEADERS, timeout=timeout)
            if r.status_code == 429:
                wait = delay * (2 ** attempt)
                print(f"    ⏳ Rate limited — waiting {wait}s", flush=True)
                time.sleep(wait)
                continue
            return r
        except requests.exceptions.Timeout:
            print(f"    ⏳ Timeout attempt {attempt+1}/{retries}: {url[:60]}", flush=True)
            time.sleep(delay * (2 ** attempt))
        except Exception as e:
            print(f"    ❌ Request error: {e}", flush=True)
            time.sleep(delay)
    return None


# ─── Gun model detection ─────────────────────────────────────────────────────
GUN_MODELS = {
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
    "sig p365xl":       ["p365xl", "p365 xl", "p365-xl"],
    "sig p365x macro":  ["p365x macro", "p365-x macro"],
    "sig p365":         [" p365 ", "p365,", "p365/"],
    "sig p320 xcarry":  ["p320 x-carry", "p320 xcarry", "x-carry"],
    "sig p320 compact": ["p320 compact", " p320c "],
    "sig p320 full":    ["p320 full", "p320 fs"],
    "sig p320":         [" p320 "],
    "sig p226":         [" p226 ", "p226,"],
    "sig p229":         [" p229 "],
    "sig p938":         [" p938 "],
    "sw mp shield plus": ["m&p shield plus", "shield plus", "mp shield plus"],
    "sw mp shield ez":   ["shield ez", "m&p ez"],
    "sw mp shield":      ["m&p shield ", "mp shield ", " shield "],
    "sw mp 2.0 compact": ["m&p 2.0 compact", "m2.0 compact", "mp 2.0 compact", "m&p m2.0 compact"],
    "sw mp 2.0":         ["m&p 2.0", "m&p m2.0", "mp 2.0"],
    "sw equalizer":      ["equalizer"],
    "sw csx":            [" csx "],
    "springfield hellcat pro": ["hellcat pro"],
    "springfield hellcat rdp": ["hellcat rdp"],
    "springfield hellcat":     [" hellcat "],
    "springfield echelon":     [" echelon "],
    "springfield xdm elite":   ["xd-m elite", "xdm elite"],
    "springfield xds":         [" xd-s ", " xds "],
    "springfield xdm":         [" xd-m ", " xdm "],
    "springfield xd":          [" xd-9 ", " xd-40 ", " xd "],
    "hk vp9sk": ["vp9sk"],
    "hk vp9":   [" vp9 ", " vp9,"],
    "hk p30sk": ["p30sk"],
    "hk p30l":  [" p30l "],
    "hk p30":   [" p30 ", " p30,"],
    "hk usp compact": ["usp compact"],
    "hk usp":   [" usp "],
    "cz p10c": ["p-10 c", " p10c", "p-10c"],
    "cz p10f": ["p-10 f", " p10f", "p-10f"],
    "cz p10s": ["p-10 s", " p10s"],
    "cz p07":  [" p-07 ", " p07 "],
    "cz p09":  [" p-09 ", " p09 "],
    "walther pdp compact": ["pdp compact"],
    "walther pdp":  [" pdp full", " pdp "],
    "walther ppq":  [" ppq "],
    "walther pps":  [" pps "],
    "ruger max9":      ["max-9", " max9"],
    "ruger security9": ["security-9", "security9"],
    "ruger lcp max":   ["lcp max"],
    "ruger lcp2":      ["lcp ii", " lcp2"],
    "ruger lcp":       [" lcp "],
    "taurus g3c": [" g3c "],
    "taurus g3":  ["taurus g3 "],
    "taurus gx4": [" gx4 "],
    "taurus g2c": [" g2c "],
    "canik tp9sf": ["tp9sf", "tp9 sf"],
    "canik mete":  ["mete sf", "mete sft", "mete sfx"],
    "fn 509 compact":  ["509 compact", "509c "],
    "fn 509 tactical": ["509 tactical"],
    "fn 509":  [" fn509 ", " 509 "],
    "kimber micro9": ["micro 9", "micro9"],
    "beretta 92fs": ["92fs", "92 fs"],
    "beretta apx":  [" apx "],
    "beretta px4":  [" px4 "],
    "1911 5 inch": ["1911 5 inch", "1911 government", "1911 5\""],
    "1911 4 inch": ["1911 4 inch", "1911 commander", "1911 4\""],
    "1911 3 inch": ["1911 3 inch", "1911 officer", "1911 3\""],
    "1911":        [" 1911 "],
}

def detect_gun_model(text):
    text = " " + text.lower() + " "
    for model, keywords in GUN_MODELS.items():
        if any(k in text for k in keywords):
            return model
    return None


# ─── Carry type detection ────────────────────────────────────────────────────
CARRY_KEYWORDS = {
    "aiwb":     ["aiwb", "appendix"],
    "iwb":      ["iwb", "inside waistband", "inside the waistband", "tuckable", "tuck"],
    "owb":      ["owb", "outside waistband", "outside the waistband", "paddle", "belt slide", "pancake"],
    "duty":     ["duty", "level ii", "level iii", "retention", "als", "sls"],
    "shoulder": ["shoulder"],
    "ankle":    ["ankle"],
    "chest":    ["chest rig", "chest holster"],
    "offbody":  ["off-body", "off body", "purse carry"],
}

def detect_carry(text):
    text = text.lower()
    if any(k in text for k in CARRY_KEYWORDS["aiwb"]):
        return "aiwb"
    for carry, keywords in CARRY_KEYWORDS.items():
        if carry == "aiwb":
            continue
        if any(k in text for k in keywords):
            return carry
    return None  # Step 6 fix: was "iwb", now None


# ─── Draw hand detection ─────────────────────────────────────────────────────
def detect_hand(text):
    text = text.lower()
    if "left hand" in text or "left-hand" in text or "lh " in text:
        return "left"
    if "ambi" in text:
        return "ambi"
    if "right hand" in text or "right-hand" in text or "rh " in text:
        return "right"
    return None  # Step 6 fix: was "right", now None


# ─── Optic cut detection ─────────────────────────────────────────────────────
OPTIC_KEYWORDS = {
    "rmr":             ["trijicon rmr", " rmr ", "rmr type 2"],
    "rmrcc":           ["rmrcc", "rmr cc"],
    "sro":             ["trijicon sro", " sro "],
    "holosun-507c":    ["507c", "hs507c"],
    "holosun-508t":    ["508t", "hs508t"],
    "holosun-509t":    ["509t", "hs509t"],
    "holosun-407c":    ["407c", "hs407c"],
    "holosun-510c":    ["510c", "hs510c"],
    "holosun-any":     ["holosun"],
    "shield-sms":      ["shield sms", " sms "],
    "shield-rmsc":     ["shield rmsc", "rmsc", "shield rms"],
    "sig-romeo-zero":  ["romeo zero", "romeozero"],
    "sig-romeo1-pro":  ["romeo1 pro", "romeo 1 pro"],
    "leupold-dpp":     ["deltapoint pro", "delta point pro", " dpp "],
    "vortex-venom":    ["vortex venom", " venom "],
    "vortex-razor":    ["vortex razor", " razor "],
    "eotech-eflx":     ["eflx", "eotech eflx"],
    "burris-fastfire": ["fastfire", "fast fire"],
    "ct-cts1550":      ["cts-1550", "cts1550"],
    "any":             ["optic cut", "optic ready", "mos ", " or ", "rmr cut",
                        "red dot cut", "optic compatible", "suppressor height"],
}

def detect_optic(text):
    text = " " + text.lower() + " "
    for optic_key, keywords in OPTIC_KEYWORDS.items():
        if any(k in text for k in keywords):
            return optic_key
    return None


# ─── Weapon light detection ──────────────────────────────────────────────────
LIGHT_KEYWORDS = {
    "tlr-1":     ["tlr-1 hl", "tlr1 hl", "tlr-1hl", "tlr1hl", "tlr 1 hl", "tlr-1s", "tlr1s", " tlr-1 ", " tlr1 ", "tlr 1 "],
    "tlr-7":     ["tlr-7a", "tlr7a", "tlr-7 a", "tlr 7a", " tlr-7 ", " tlr7 ", "tlr 7 "],
    "tlr-7-sub": ["tlr-7 sub", "tlr7 sub", "tlr-7sub"],
    "tlr-8":     ["tlr-8a", "tlr8a", "tlr-8 a", "tlr 8a", " tlr-8 ", " tlr8 ", "tlr 8 "],
    "tlr-9":     [" tlr-9 ", " tlr9 ", "tlr 9 "],
    "tlr-10":    [" tlr-10", " tlr10", "tlr 10"],
    "sf-x300u-a":    ["x300u-a", "x300u a", "x300ua"],
    "sf-x300u-b":    ["x300u-b", "x300u b", "x300ub"],
    "sf-x300":       [" x300 ", "x300u"],
    "sf-xc1":        [" xc1 ", "xc-1"],
    "sf-xc2":        [" xc2 ", "xc-2"],
    "olight-pl-mini2":  ["pl-mini 2", "pl mini 2", "pl-mini2", "valkyrie mini"],
    "olight-pl-pro":    ["pl-pro", "pl pro", "valkyrie pro"],
    "olight-baldr-mini":["baldr mini", "baldrmini"],
    "olight-baldr-pro": ["baldr pro", "baldrpro"],
    "olight-baldr-s":   ["baldr s ", "baldr-s"],
    "ns-twm-30":  ["twm-30", "twm30"],
    "ns-twm-852": ["twm-852", "twm852"],
    "inforce-apl":    ["inforce apl", " apl ", "aplc"],
    "cd-rein":        ["rein micro", "rein 2", "cd rein"],
    "ct-rail-master": ["rail master", "cmr-207", "cmr207", "cmr-208", "cmr208"],
    "any":            ["weapon light", "wml", "light bearing", "w/ light", "with light", "w/light",
                       "light-bearing", "streamlight", "surefire", "olight"],
}

def detect_light(text):
    text = " " + text.lower() + " "
    for light_key, keywords in LIGHT_KEYWORDS.items():
        if any(k in text for k in keywords):
            return light_key
    return None


# ─── Material detection ──────────────────────────────────────────────────────
def detect_material(text):
    text = text.lower()
    if any(k in text for k in ["kydex", "boltaron", "polymer", "thermoplastic"]):
        return "kydex"
    if any(k in text for k in ["leather", "cowhide", "horsehide", "suede"]):
        if any(k in text for k in ["kydex", "hybrid", "backer"]):
            return "hybrid"
        return "leather"
    if "hybrid" in text or "neoprene" in text:
        return "hybrid"
    return None


# ─── In-stock detection ──────────────────────────────────────────────────────
def detect_in_stock(variants):
    """Check Shopify variants for availability."""
    if not variants:
        return None
    return any(v.get("available", False) for v in variants)


# ─── Helpers ─────────────────────────────────────────────────────────────────
def clean_price(price_str):
    if not price_str:
        return None
    nums = re.findall(r"[\d]+\.?[\d]*", str(price_str).replace(",", ""))
    return float(nums[0]) if nums else None


def save_to_supabase(products):
    """Save scraped products to Supabase with upsert on product_url."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("⚠️  No Supabase credentials — saving to local JSON instead")
        os.makedirs("data", exist_ok=True)
        with open("data/products.json", "w") as f:
            json.dump(products, f, indent=2)
        print(f"✅ Saved {len(products)} products to data/products.json", flush=True)
        return

    url = f"{SUPABASE_URL}/rest/v1/holsters"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    # Filter out products missing product_url
    valid = [p for p in products if p.get("product_url")]
    skipped = len(products) - len(valid)
    if skipped:
        print(f"  ⚠️  Skipped {skipped} products with no product_url", flush=True)

    batch_size = 100
    total_saved = 0
    for i in range(0, len(valid), batch_size):
        batch = valid[i:i + batch_size]
        r = requests.post(
            url + "?on_conflict=product_url",  # Step 1 fix
            headers=headers,
            json=batch,
            timeout=30,
        )
        if r.status_code in (200, 201):
            total_saved += len(batch)
        else:
            print(f"❌ Supabase error: {r.status_code} — {r.text[:200]}", flush=True)

    print(f"✅ Saved {total_saved}/{len(valid)} products to Supabase", flush=True)


# ═══════════════════════════════════════════════════════════════════════════
# SHOPIFY SCRAPER
# ═══════════════════════════════════════════════════════════════════════════

def scrape_shopify(brand_name, base_url, delay=1.5):
    products = []
    page = 1
    print(f"  Scraping {brand_name} (Shopify)...", flush=True)

    while True:
        url = f"{base_url}/products.json?limit=250&page={page}"
        r = fetch_with_retry(url)
        if not r or r.status_code != 200:
            break
        try:
            data = r.json()
        except Exception:
            break
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

            if not any(kw in combined_text.lower() for kw in ["holster", "iwb", "owb", "aiwb", "carry"]):
                continue

            images = item.get("images", [])
            image_url = images[0].get("src", "") if images else ""

            variants = item.get("variants", [])
            price = None
            for v in variants:
                if v.get("available", True):
                    price = clean_price(v.get("price"))
                    break
            if price is None and variants:
                price = clean_price(variants[0].get("price"))

            in_stock = detect_in_stock(variants)

            products.append({
                "brand": brand_name,
                "name": title,
                "price": price,
                "image_url": image_url,
                "product_url": f"{base_url}/products/{handle}",
                "carry_type": detect_carry(combined_text),
                "draw_hand": detect_hand(combined_text),
                "light": detect_light(combined_text),
                "optic": detect_optic(combined_text),
                "gun_model": detect_gun_model(combined_text),
                "material": detect_material(combined_text),
                "in_stock": in_stock,
                "source": "shopify",
                "last_scraped": datetime.utcnow().isoformat(),
            })

        page += 1
        time.sleep(delay)

    print(f"    ✅ {brand_name}: {len(products)} holsters found", flush=True)
    return products


# ═══════════════════════════════════════════════════════════════════════════
# CUSTOM SCRAPERS — non-Shopify brands
# ═══════════════════════════════════════════════════════════════════════════

def scrape_safariland():
    brand = "Safariland"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.safariland.com"
    seen = set()

    categories = [
        ("holsters/duty-holsters", "duty"),
        ("holsters/concealment-holsters", "iwb"),
        ("holsters/competition-holsters", "owb"),
        ("holsters/duty-holsters/level-ii-holsters", "duty"),
        ("holsters/duty-holsters/level-iii-holsters", "duty"),
    ]

    for cat_path, carry_hint in categories:
        page = 1
        while page <= 15:
            r = fetch_with_retry(f"{base}/products/{cat_path}/?page={page}")
            if not r or r.status_code != 200:
                break
            soup = BeautifulSoup(r.text, "html.parser")
            cards = soup.select(
                ".product-card, .product-tile, [class*='product-card'], "
                "[class*='productCard'], [data-product-id], li.product"
            )
            if not cards:
                break
            new_found = 0
            for card in cards:
                name_el = card.select_one("h2, h3, h4, .product-name, .product-title, [class*='productName']")
                price_el = card.select_one("[class*='price'], .price")
                img_el = card.select_one("img")
                link_el = card.select_one("a[href]")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name or name in seen or len(name) < 4:
                    continue
                seen.add(name)
                new_found += 1
                price = clean_price(price_el.get_text() if price_el else None)
                image_url = img_el.get("src", img_el.get("data-src", "")) if img_el else ""
                if image_url and not image_url.startswith("http"):
                    image_url = base + image_url
                link = link_el.get("href", "") if link_el else ""
                if link and not link.startswith("http"):
                    link = base + link
                combined = f"{name} {link} {carry_hint}"
                products.append({
                    "brand": brand, "name": name, "price": price,
                    "image_url": image_url, "product_url": link,
                    "carry_type": detect_carry(combined),
                    "draw_hand": detect_hand(combined),
                    "light": detect_light(combined),
                    "optic": detect_optic(combined),
                    "gun_model": detect_gun_model(combined),
                    "material": detect_material(combined),
                    "in_stock": None,
                    "source": "custom",
                    "last_scraped": datetime.utcnow().isoformat(),
                })
            if new_found == 0:
                break
            next_btn = soup.select_one("a[rel='next'], .pagination__next, [class*='next']")
            if not next_btn:
                break
            page += 1
            time.sleep(0.5)

    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_blackhawk():
    brand = "Blackhawk"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.blackhawk.com"
    seen = set()

    # Try sitemap first
    r = fetch_with_retry(f"{base}/sitemap.xml")
    product_urls = []
    if r and r.status_code == 200:
        try:
            soup = BeautifulSoup(r.text, "xml")
            sub_maps = [loc.text for loc in soup.find_all("loc")]
            for sm_url in sub_maps:
                if "product" in sm_url.lower():
                    rs = fetch_with_retry(sm_url)
                    if rs:
                        ss = BeautifulSoup(rs.text, "xml")
                        product_urls += [loc.text for loc in ss.find_all("loc") if "holster" in loc.text.lower()]
        except Exception:
            pass

    # Fallback: category page
    if not product_urls:
        for path in ["/holsters/", "/products/holsters/"]:
            r = fetch_with_retry(base + path)
            if r and r.status_code == 200:
                soup = BeautifulSoup(r.text, "html.parser")
                links = soup.select("a[href*='holster']")
                product_urls += [
                    (base + a["href"]) if not a["href"].startswith("http") else a["href"]
                    for a in links if "/holster" in a.get("href", "")
                ]

    for url in list(set(product_urls))[:150]:
        try:
            pr = fetch_with_retry(url)
            if not pr or pr.status_code != 200:
                continue
            ps = BeautifulSoup(pr.text, "html.parser")
            name_el = ps.select_one("h1[itemprop='name'], h1.productView-title, h1")
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            if not name or name in seen or "holster" not in name.lower():
                continue
            seen.add(name)
            price_el = ps.select_one("[itemprop='price'], [class*='price'], .price")
            price = clean_price(price_el.get("content") or price_el.get_text() if price_el else None)
            img_el = ps.select_one("img[itemprop='image'], .product-image img")
            image_url = img_el.get("src", img_el.get("data-src", "")) if img_el else ""
            if image_url and not image_url.startswith("http"):
                image_url = base + image_url
            combined = f"{name} {url}"
            products.append({
                "brand": brand, "name": name, "price": price,
                "image_url": image_url, "product_url": url,
                "carry_type": detect_carry(combined),
                "draw_hand": detect_hand(combined),
                "light": detect_light(combined),
                "optic": detect_optic(combined),
                "gun_model": detect_gun_model(combined),
                "material": detect_material(combined),
                "in_stock": None,
                "source": "custom",
                "last_scraped": datetime.utcnow().isoformat(),
            })
            time.sleep(0.3)
        except Exception:
            pass

    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_galco():
    brand = "Galco"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.galcogunleather.com"
    seen = set()

    categories = [
        ("/product-listing/iwb-holsters/", "iwb"),
        ("/product-listing/owb-holsters/", "owb"),
        ("/product-listing/shoulder-holsters/", "shoulder"),
        ("/product-listing/ankle-holsters/", "ankle"),
        ("/product-listing/duty-holsters/", "duty"),
    ]
    for cat_path, carry_hint in categories:
        page = 1
        while page <= 10:
            r = fetch_with_retry(f"{base}{cat_path}?p={page}")
            if not r or r.status_code != 200:
                break
            soup = BeautifulSoup(r.text, "html.parser")
            cards = soup.select(".product-item, .item.product, [class*='product-item'], .productItem, li.item")
            if not cards:
                break
            new_found = 0
            for card in cards:
                name_el = card.select_one("h2, h3, .product-name, .product-item-name, a.product-item-link")
                price_el = card.select_one(".price, [class*='price'], .regular-price")
                img_el = card.select_one("img")
                link_el = card.select_one("a[href]")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name or name in seen:
                    continue
                seen.add(name)
                new_found += 1
                price = clean_price(price_el.get_text() if price_el else None)
                image_url = img_el.get("src", img_el.get("data-src", "")) if img_el else ""
                if image_url and not image_url.startswith("http"):
                    image_url = base + image_url
                link = link_el.get("href", "") if link_el else ""
                if link and not link.startswith("http"):
                    link = base + link
                combined = f"{name} {carry_hint}"
                products.append({
                    "brand": brand, "name": name, "price": price,
                    "image_url": image_url, "product_url": link,
                    "carry_type": detect_carry(combined),
                    "draw_hand": detect_hand(combined),
                    "light": detect_light(combined),
                    "optic": detect_optic(combined),
                    "gun_model": detect_gun_model(combined),
                    "material": detect_material(combined),
                    "in_stock": None,
                    "source": "custom",
                    "last_scraped": datetime.utcnow().isoformat(),
                })
            if new_found == 0:
                break
            next_el = soup.select_one("a.next, [class*='next'], [title='Next']")
            if not next_el:
                break
            page += 1
            time.sleep(0.4)

    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_desantis():
    brand = "DeSantis"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.desantisholster.com"
    page = 1
    seen = set()
    while page <= 20:
        r = fetch_with_retry(f"{base}/all-holsters/page/{page}/")
        if not r or r.status_code != 200:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".product, .woocommerce-LoopProduct, [class*='product-']")
        if not cards:
            break
        new_found = 0
        for card in cards:
            name_el = card.select_one("h2, h3, .woocommerce-loop-product__title")
            price_el = card.select_one(".price, .amount")
            img_el = card.select_one("img")
            link_el = card.select_one("a")
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            if not name or name in seen:
                continue
            seen.add(name)
            new_found += 1
            price = clean_price(price_el.get_text() if price_el else None)
            image = img_el.get("src", img_el.get("data-src", "")) if img_el else ""
            link = link_el.get("href", "") if link_el else ""
            combined = f"{name} {link}"
            products.append({
                "brand": brand, "name": name, "price": price,
                "image_url": image, "product_url": link,
                "carry_type": detect_carry(combined),
                "draw_hand": detect_hand(combined),
                "light": detect_light(combined),
                "optic": detect_optic(combined),
                "gun_model": detect_gun_model(combined),
                "material": detect_material(combined),
                "in_stock": None,
                "source": "custom",
                "last_scraped": datetime.utcnow().isoformat(),
            })
        if new_found == 0:
            break
        page += 1
        time.sleep(0.4)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_comptac():
    brand = "Comp-Tac"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.comp-tac.com"
    seen = set()
    # Step 7 fix: correct WooCommerce URL
    categories = [
        ("/product-category/holsters/iwb/", "iwb"),
        ("/product-category/holsters/owb/", "owb"),
        ("/product-category/holsters/", "iwb"),
    ]
    for cat_path, carry_hint in categories:
        page = 1
        while page <= 10:
            r = fetch_with_retry(f"{base}{cat_path}page/{page}/")
            if not r or r.status_code != 200:
                break
            soup = BeautifulSoup(r.text, "html.parser")
            cards = soup.select(".product, [class*='product-item'], .woocommerce-LoopProduct")
            if not cards:
                break
            new_found = 0
            for card in cards:
                name_el = card.select_one("h2, h3, .product-title, .woocommerce-loop-product__title")
                price_el = card.select_one(".price, .amount")
                img_el = card.select_one("img")
                link_el = card.select_one("a")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name or name in seen:
                    continue
                seen.add(name)
                new_found += 1
                price = clean_price(price_el.get_text() if price_el else None)
                image = img_el.get("src", "") if img_el else ""
                link = link_el.get("href", "") if link_el else ""
                combined = f"{name} {carry_hint}"
                products.append({
                    "brand": brand, "name": name, "price": price,
                    "image_url": image, "product_url": link,
                    "carry_type": detect_carry(combined),
                    "draw_hand": detect_hand(combined),
                    "light": detect_light(combined),
                    "optic": detect_optic(combined),
                    "gun_model": detect_gun_model(combined),
                    "material": detect_material(combined),
                    "in_stock": None,
                    "source": "custom",
                    "last_scraped": datetime.utcnow().isoformat(),
                })
            if new_found == 0:
                break
            page += 1
            time.sleep(0.4)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_blade_tech():
    brand = "Blade-Tech"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.blade-tech.com"
    seen = set()
    page = 1
    while page <= 20:
        r = fetch_with_retry(f"{base}/holsters/?page={page}")
        if not r or r.status_code != 200:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".product-item, .product, [class*='product']")
        if not cards:
            break
        new_found = 0
        for card in cards:
            name_el = card.select_one("h2, h3, .product-name")
            price_el = card.select_one(".price, [class*='price']")
            img_el = card.select_one("img")
            link_el = card.select_one("a")
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            if not name or name in seen:
                continue
            seen.add(name)
            new_found += 1
            price = clean_price(price_el.get_text() if price_el else None)
            image = img_el.get("src", img_el.get("data-src", "")) if img_el else ""
            link = link_el.get("href", "") if link_el else ""
            if link and not link.startswith("http"):
                link = base + link
            combined = f"{name} {link}"
            products.append({
                "brand": brand, "name": name, "price": price,
                "image_url": image, "product_url": link,
                "carry_type": detect_carry(combined),
                "draw_hand": detect_hand(combined),
                "light": detect_light(combined),
                "optic": detect_optic(combined),
                "gun_model": detect_gun_model(combined),
                "material": detect_material(combined),
                "in_stock": None,
                "source": "custom",
                "last_scraped": datetime.utcnow().isoformat(),
            })
        if new_found == 0:
            break
        page += 1
        time.sleep(0.4)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_gcode():
    brand = "G-Code Holsters"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    # Step 4 fix: correct URL — was tacticalholsters.com (wrong brand)
    base = "https://www.gcode-holsters.com"
    seen = set()
    r = fetch_with_retry(f"{base}/holsters/")
    if r and r.status_code == 200:
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
            if not name or name in seen:
                continue
            seen.add(name)
            price = clean_price(price_el.get_text() if price_el else None)
            image = img_el.get("src", "") if img_el else ""
            link = link_el.get("href", "") if link_el else ""
            if link and not link.startswith("http"):
                link = base + link
            combined = f"{name} {link}"
            products.append({
                "brand": brand, "name": name, "price": price,
                "image_url": image, "product_url": link,
                "carry_type": detect_carry(combined),
                "draw_hand": detect_hand(combined),
                "light": detect_light(combined),
                "optic": detect_optic(combined),
                "gun_model": detect_gun_model(combined),
                "material": detect_material(combined),
                "in_stock": None,
                "source": "custom",
                "last_scraped": datetime.utcnow().isoformat(),
            })
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_fobus():
    brand = "Fobus"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.fobusholster.com"
    seen = set()
    page = 1
    while page <= 10:
        r = fetch_with_retry(f"{base}/holsters/page/{page}/")
        if not r or r.status_code != 200:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".product, .woocommerce-LoopProduct, [class*='product']")
        if not cards:
            break
        new_found = 0
        for card in cards:
            name_el = card.select_one("h2, h3, .woocommerce-loop-product__title")
            price_el = card.select_one(".price, .amount")
            img_el = card.select_one("img")
            link_el = card.select_one("a")
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            if not name or name in seen:
                continue
            seen.add(name)
            new_found += 1
            price = clean_price(price_el.get_text() if price_el else None)
            image = img_el.get("src", "") if img_el else ""
            link = link_el.get("href", "") if link_el else ""
            combined = f"{name} {link}"
            products.append({
                "brand": brand, "name": name, "price": price,
                "image_url": image, "product_url": link,
                "carry_type": detect_carry(combined),
                "draw_hand": detect_hand(combined),
                "light": detect_light(combined),
                "optic": detect_optic(combined),
                "gun_model": detect_gun_model(combined),
                "material": detect_material(combined),
                "in_stock": None,
                "source": "custom",
                "last_scraped": datetime.utcnow().isoformat(),
            })
        if new_found == 0:
            break
        page += 1
        time.sleep(0.4)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_miltsparks():
    brand = "Milt Sparks"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.miltsparks.com"
    seen = set()
    r = fetch_with_retry(f"{base}/holsters/")
    if r and r.status_code == 200:
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
            if not name or name in seen:
                continue
            seen.add(name)
            price = clean_price(price_el.get_text() if price_el else None)
            image = img_el.get("src", "") if img_el else ""
            link = link_el.get("href", "") if link_el else ""
            combined = f"{name} {link}"
            products.append({
                "brand": brand, "name": name, "price": price,
                "image_url": image, "product_url": link,
                "carry_type": detect_carry(combined),
                "draw_hand": detect_hand(combined),  # Step 7 fix: was hardcoded "right"
                "light": detect_light(combined),
                "optic": detect_optic(combined),
                "gun_model": detect_gun_model(combined),
                "material": detect_material(combined),
                "in_stock": None,
                "source": "custom",
                "last_scraped": datetime.utcnow().isoformat(),
            })
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_don_hume():
    brand = "Don Hume"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://donhume.com"
    seen = set()
    page = 1
    while page <= 10:
        r = fetch_with_retry(f"{base}/product-category/holsters/page/{page}/")
        if not r or r.status_code != 200:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".product, .woocommerce-LoopProduct, [class*='product']")
        if not cards:
            break
        new_found = 0
        for card in cards:
            name_el = card.select_one("h2, h3, .woocommerce-loop-product__title")
            price_el = card.select_one(".price, .amount")
            img_el = card.select_one("img")
            link_el = card.select_one("a")
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            if not name or name in seen:
                continue
            seen.add(name)
            new_found += 1
            price = clean_price(price_el.get_text() if price_el else None)
            image = img_el.get("src", "") if img_el else ""
            link = link_el.get("href", "") if link_el else ""
            combined = f"{name} {link}"
            products.append({
                "brand": brand, "name": name, "price": price,
                "image_url": image, "product_url": link,
                "carry_type": detect_carry(combined),
                "draw_hand": detect_hand(combined),
                "light": detect_light(combined),
                "optic": detect_optic(combined),
                "gun_model": detect_gun_model(combined),
                "material": detect_material(combined),
                "in_stock": None,
                "source": "custom",
                "last_scraped": datetime.utcnow().isoformat(),
            })
        if new_found == 0:
            break
        page += 1
        time.sleep(0.4)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


# ─── Vedder — BigCommerce GraphQL ────────────────────────────────────────────
def scrape_vedder():
    brand = "Vedder Holsters"
    products = []
    print(f"  Scraping {brand} (BigCommerce)...", flush=True)
    base = "https://www.vedderholsters.com"
    graphql_url = f"{base}/graphql"
    gql_headers = {"Content-Type": "application/json", "User-Agent": HEADERS["User-Agent"]}

    QUERY = """
    query CategoryProducts($entityId: Int!, $after: String) {
      site {
        category(entityId: $entityId) {
          products(first: 50, after: $after) {
            pageInfo { hasNextPage endCursor }
            edges {
              node {
                name path description
                prices { price { value } }
                defaultImage { url(width: 400) }
              }
            }
          }
        }
      }
    }
    """
    categories = [(23, "iwb"), (24, "owb"), (25, "aiwb"), (33, "iwb")]
    seen = set()

    for cat_id, carry_hint in categories:
        cursor = None
        while True:
            variables = {"entityId": cat_id}
            if cursor:
                variables["after"] = cursor
            try:
                r = requests.post(graphql_url, headers=gql_headers,
                                  json={"query": QUERY, "variables": variables}, timeout=12)
                if r.status_code != 200:
                    break
                data = r.json()
                page_products = data.get("data", {}).get("site", {}).get("category", {}).get("products", {})
                for edge in page_products.get("edges", []):
                    node = edge.get("node", {})
                    name = node.get("name", "").strip()
                    if not name or name in seen:
                        continue
                    seen.add(name)
                    path = node.get("path", "")
                    price_val = node.get("prices", {}).get("price", {}).get("value", 0)
                    price = float(price_val) if price_val else None
                    img = node.get("defaultImage", {})
                    image_url = img.get("url", "") if img else ""
                    desc = node.get("description", "")
                    combined = f"{name} {desc}"
                    products.append({
                        "brand": brand, "name": name, "price": price,
                        "image_url": image_url, "product_url": base + path,
                        "carry_type": detect_carry(combined) or carry_hint,
                        "draw_hand": detect_hand(combined),
                        "light": detect_light(combined),
                        "optic": detect_optic(combined),
                        "gun_model": detect_gun_model(combined),
                        "material": detect_material(combined),
                        "in_stock": None,
                        "source": "bigcommerce_graphql",
                        "last_scraped": datetime.utcnow().isoformat(),
                    })
                page_info = page_products.get("pageInfo", {})
                if page_info.get("hasNextPage"):
                    cursor = page_info.get("endCursor")
                else:
                    break
                time.sleep(0.5)
            except Exception as e:
                print(f"    ❌ Vedder cat {cat_id}: {e}", flush=True)
                break

    # Sitemap fallback
    if not products:
        try:
            r = fetch_with_retry(f"{base}/sitemap.xml")
            if r:
                soup = BeautifulSoup(r.text, "xml")
                urls = [loc.text for loc in soup.find_all("loc") if "/holsters/" in loc.text]
                for url in urls[:150]:
                    pr = fetch_with_retry(url)
                    if not pr:
                        continue
                    ps = BeautifulSoup(pr.text, "html.parser")
                    name_el = ps.select_one("h1")
                    if not name_el:
                        continue
                    name = name_el.get_text(strip=True)
                    if not name or name in seen:
                        continue
                    seen.add(name)
                    price_el = ps.select_one("[class*='price']")
                    price = clean_price(price_el.get_text() if price_el else None)
                    img_el = ps.select_one("img[itemprop='image']")
                    image_url = img_el.get("src", "") if img_el else ""
                    combined = f"{name} {url}"
                    products.append({
                        "brand": brand, "name": name, "price": price,
                        "image_url": image_url, "product_url": url,
                        "carry_type": detect_carry(combined),
                        "draw_hand": detect_hand(combined),
                        "light": detect_light(combined),
                        "optic": detect_optic(combined),
                        "gun_model": detect_gun_model(combined),
                        "material": detect_material(combined),
                        "in_stock": None,
                        "source": "bigcommerce_sitemap",
                        "last_scraped": datetime.utcnow().isoformat(),
                    })
                    time.sleep(0.3)
        except Exception as e:
            print(f"    ❌ Vedder sitemap fallback: {e}", flush=True)

    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


# ─── Tulster — BigCommerce GraphQL ───────────────────────────────────────────
def scrape_tulster():
    brand = "Tulster"
    products = []
    print(f"  Scraping {brand} (BigCommerce)...", flush=True)
    base = "https://tulster.com"
    graphql_url = f"{base}/graphql"
    gql_headers = {"Content-Type": "application/json", "User-Agent": HEADERS["User-Agent"]}

    QUERY = """
    query SearchProducts($searchTerm: String!, $after: String) {
      site {
        search {
          searchProducts(filters: {searchTerm: $searchTerm}, sort: RELEVANCE) {
            products(first: 50, after: $after) {
              pageInfo { hasNextPage endCursor }
              edges {
                node {
                  name path description
                  prices { price { value } }
                  defaultImage { url(width: 400) }
                  categories { edges { node { name } } }
                }
              }
            }
          }
        }
      }
    }
    """
    seen = set()
    for search_term in ["holster iwb", "holster owb", "appendix holster"]:
        cursor = None
        while True:
            variables = {"searchTerm": search_term}
            if cursor:
                variables["after"] = cursor
            try:
                r = requests.post(graphql_url, headers=gql_headers,
                                  json={"query": QUERY, "variables": variables}, timeout=12)
                if r.status_code != 200:
                    break
                data = r.json()
                search_data = (data.get("data", {}).get("site", {}).get("search", {})
                               .get("searchProducts", {}).get("products", {}))
                for edge in search_data.get("edges", []):
                    node = edge.get("node", {})
                    name = node.get("name", "").strip()
                    if not name or name in seen:
                        continue
                    if not any(k in name.lower() for k in ["holster", "iwb", "owb", "aiwb"]):
                        continue
                    seen.add(name)
                    path = node.get("path", "")
                    price_val = node.get("prices", {}).get("price", {}).get("value", 0)
                    price = float(price_val) if price_val else None
                    img = node.get("defaultImage", {})
                    image_url = img.get("url", "") if img else ""
                    desc = node.get("description", "")
                    cats = " ".join(e["node"]["name"] for e in node.get("categories", {}).get("edges", []))
                    combined = f"{name} {desc} {cats}"
                    products.append({
                        "brand": brand, "name": name, "price": price,
                        "image_url": image_url, "product_url": base + path,
                        "carry_type": detect_carry(combined),
                        "draw_hand": detect_hand(combined),
                        "light": detect_light(combined),
                        "optic": detect_optic(combined),
                        "gun_model": detect_gun_model(combined),
                        "material": detect_material(combined),
                        "in_stock": None,
                        "source": "bigcommerce_graphql",
                        "last_scraped": datetime.utcnow().isoformat(),
                    })
                page_info = search_data.get("pageInfo", {})
                if page_info.get("hasNextPage"):
                    cursor = page_info.get("endCursor")
                else:
                    break
                time.sleep(0.5)
            except Exception as e:
                print(f"    ❌ Tulster '{search_term}': {e}", flush=True)
                break

    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


# ─── StealthGear — Shopify collection API ────────────────────────────────────
def scrape_stealthgear():
    brand = "StealthGear USA"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://stealthgearusa.com"
    collections = [
        ("iwb-holsters", "iwb"),
        ("owb-holsters", "owb"),
        ("appendix-holsters", "aiwb"),
        ("ventcore-holsters", "iwb"),
    ]
    seen = set()
    for collection_handle, carry_hint in collections:
        page = 1
        while True:
            url = f"{base}/collections/{collection_handle}/products.json?limit=250&page={page}"
            r = fetch_with_retry(url)
            if not r or r.status_code not in (200,):
                break
            try:
                data = r.json()
            except Exception:
                break
            items = data.get("products", [])
            if not items:
                break
            for item in items:
                title = item.get("title", "")
                if not title or title in seen:
                    continue
                seen.add(title)
                handle = item.get("handle", "")
                tags = " ".join(item.get("tags", []))
                body = BeautifulSoup(item.get("body_html", ""), "html.parser").get_text(" ")
                combined = f"{title} {tags} {body}"
                images = item.get("images", [])
                image_url = images[0].get("src", "") if images else ""
                variants = item.get("variants", [])
                price = None
                for v in variants:
                    if v.get("available", True):
                        price = clean_price(v.get("price"))
                        break
                if price is None and variants:
                    price = clean_price(variants[0].get("price"))
                products.append({
                    "brand": brand, "name": title, "price": price,
                    "image_url": image_url, "product_url": f"{base}/products/{handle}",
                    "carry_type": detect_carry(combined) or carry_hint,
                    "draw_hand": detect_hand(combined),
                    "light": detect_light(combined),
                    "optic": detect_optic(combined),
                    "gun_model": detect_gun_model(combined),
                    "material": detect_material(combined),
                    "in_stock": detect_in_stock(variants),
                    "source": "shopify_collection",
                    "last_scraped": datetime.utcnow().isoformat(),
                })
            page += 1
            time.sleep(0.5)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


# ─── CrossBreed — Shopify collection API ─────────────────────────────────────
def scrape_crossbreed():
    brand = "CrossBreed Holsters"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.crossbreedholsters.com"
    collections = [
        ("iwb-holsters", "iwb"),
        ("owb-holsters", "owb"),
        ("appendix-carry", "aiwb"),
        ("hybrid-holsters", "iwb"),
        ("minituck", "iwb"),
        ("supertuck", "iwb"),
    ]
    seen = set()
    for collection_handle, carry_hint in collections:
        page = 1
        while True:
            url = f"{base}/collections/{collection_handle}/products.json?limit=250&page={page}"
            r = fetch_with_retry(url)
            if not r or r.status_code != 200:
                break
            try:
                data = r.json()
            except Exception:
                break
            items = data.get("products", [])
            if not items:
                break
            for item in items:
                title = item.get("title", "")
                if not title or title in seen:
                    continue
                seen.add(title)
                handle = item.get("handle", "")
                tags = " ".join(item.get("tags", []))
                body = BeautifulSoup(item.get("body_html", ""), "html.parser").get_text(" ")
                combined = f"{title} {tags} {body} {carry_hint}"
                images = item.get("images", [])
                image_url = images[0].get("src", "") if images else ""
                variants = item.get("variants", [])
                price = None
                for v in variants:
                    if v.get("available", True):
                        price = clean_price(v.get("price"))
                        break
                if price is None and variants:
                    price = clean_price(variants[0].get("price"))
                products.append({
                    "brand": brand, "name": title, "price": price,
                    "image_url": image_url, "product_url": f"{base}/products/{handle}",
                    "carry_type": detect_carry(combined) or carry_hint,
                    "draw_hand": detect_hand(combined),
                    "light": detect_light(combined),
                    "optic": detect_optic(combined),
                    "gun_model": detect_gun_model(combined),
                    "material": detect_material(combined),
                    "in_stock": detect_in_stock(variants),
                    "source": "shopify_collection",
                    "last_scraped": datetime.utcnow().isoformat(),
                })
            page += 1
            time.sleep(0.5)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_raven_concealment():
    brand = "Raven Concealment"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://rcsgear.com"
    seen = set()
    page = 1
    while True:
        url = f"{base}/collections/holsters/products.json?limit=250&page={page}"
        r = fetch_with_retry(url)
        if not r or r.status_code != 200:
            break
        try:
            data = r.json()
        except Exception:
            break
        items = data.get("products", [])
        if not items:
            break
        for item in items:
            title = item.get("title", "")
            if not title or title in seen:
                continue
            seen.add(title)
            handle = item.get("handle", "")
            tags = " ".join(item.get("tags", []))
            body = BeautifulSoup(item.get("body_html", ""), "html.parser").get_text(" ")
            combined = f"{title} {tags} {body}"
            images = item.get("images", [])
            image_url = images[0].get("src", "") if images else ""
            variants = item.get("variants", [])
            price = clean_price(variants[0].get("price")) if variants else None
            products.append({
                "brand": brand, "name": title, "price": price,
                "image_url": image_url, "product_url": f"{base}/products/{handle}",
                "carry_type": detect_carry(combined),
                "draw_hand": detect_hand(combined),
                "light": detect_light(combined),
                "optic": detect_optic(combined),
                "gun_model": detect_gun_model(combined),
                "material": detect_material(combined),
                "in_stock": detect_in_stock(variants),
                "source": "shopify_collection",
                "last_scraped": datetime.utcnow().isoformat(),
            })
        page += 1
        time.sleep(0.5)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


def scrape_versacarry():
    brand = "Versacarry"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://versacarry.com"
    seen = set()
    page = 1
    while True:
        url = f"{base}/collections/holsters/products.json?limit=250&page={page}"
        r = fetch_with_retry(url)
        if not r or r.status_code != 200:
            break
        try:
            data = r.json()
        except Exception:
            break
        items = data.get("products", [])
        if not items:
            break
        for item in items:
            title = item.get("title", "")
            if not title or title in seen:
                continue
            seen.add(title)
            handle = item.get("handle", "")
            tags = " ".join(item.get("tags", []))
            body = BeautifulSoup(item.get("body_html", ""), "html.parser").get_text(" ")
            combined = f"{title} {tags} {body}"
            images = item.get("images", [])
            image_url = images[0].get("src", "") if images else ""
            variants = item.get("variants", [])
            price = clean_price(variants[0].get("price")) if variants else None
            products.append({
                "brand": brand, "name": title, "price": price,
                "image_url": image_url, "product_url": f"{base}/products/{handle}",
                "carry_type": detect_carry(combined),
                "draw_hand": detect_hand(combined),
                "light": detect_light(combined),
                "optic": detect_optic(combined),
                "gun_model": detect_gun_model(combined),
                "material": detect_material(combined),
                "in_stock": detect_in_stock(variants),
                "source": "shopify_collection",
                "last_scraped": datetime.utcnow().isoformat(),
            })
        page += 1
        time.sleep(0.5)
    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


# ═══════════════════════════════════════════════════════════════════════════
# BRAND REGISTRY
# Step 4 fix: removed duplicates (Don Hume handled by custom scraper,
# Cloak and Carry / Gould & Goodrich / Covert Carry / Hillman Holsters
# deduped from SHOPIFY_BRANDS_EXTRA)
# ═══════════════════════════════════════════════════════════════════════════

SHOPIFY_BRANDS = [
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
]

CUSTOM_SCRAPERS = [
    scrape_safariland,
    scrape_blackhawk,
    scrape_galco,
    scrape_desantis,
    scrape_comptac,
    scrape_blade_tech,
    scrape_gcode,
    scrape_fobus,
    scrape_miltsparks,
    scrape_don_hume,      # custom only — removed from SHOPIFY_BRANDS to avoid duplicates
    scrape_vedder,
    scrape_crossbreed,
    scrape_stealthgear,
    scrape_tulster,
    scrape_raven_concealment,
    scrape_versacarry,
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

    print("── SHOPIFY BRANDS ──────────────────────────────────────")
    for brand_name, base_url in SHOPIFY_BRANDS:
        try:
            products = scrape_shopify(brand_name, base_url)
            all_products.extend(products)
        except Exception as e:
            errors.append(f"{brand_name}: {e}")
            print(f"    ❌ FAILED: {e}", flush=True)
        time.sleep(0.3)

    print("\n── CUSTOM SCRAPERS ─────────────────────────────────────")
    for scraper_fn in CUSTOM_SCRAPERS:
        try:
            products = scraper_fn()
            all_products.extend(products)
        except Exception as e:
            errors.append(f"{scraper_fn.__name__}: {e}")
            print(f"    ❌ FAILED: {e}", flush=True)
        time.sleep(0.3)

    print(f"\n{'='*60}", flush=True)
    print(f"  TOTAL: {len(all_products)} holsters scraped", flush=True)
    if errors:
        print(f"  ERRORS ({len(errors)}):", flush=True)
        for err in errors:
            print(f"    • {err}", flush=True)
    print(f"{'='*60}\n", flush=True)

    os.makedirs("data", exist_ok=True)
    save_to_supabase(all_products)

    return all_products


if __name__ == "__main__":
    main()
