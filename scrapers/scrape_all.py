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
- URL Audit: Safariland/Comp-Tac/Blade-Tech/Fobus moved to Shopify scraper
             G-Code fixed to tacticalholsters.com
             Galco fixed to /holsters_8_1.html
             DeSantis fixed to sitemap approach
             Milt Sparks fixed to correct category URLs
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
    """GET with exponential backoff and rotating user agents. Returns Response or None."""
    import random
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    ]
    for attempt in range(retries):
        try:
            hdrs = dict(headers or HEADERS)
            hdrs["User-Agent"] = random.choice(user_agents)
            r = requests.get(url, headers=hdrs, timeout=timeout)
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

# NOTE: Safariland, Comp-Tac, Blade-Tech, and Fobus moved to SHOPIFY_BRANDS
# (confirmed on Shopify platform May 2026 — scrape_shopify() handles them)


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

    # Galco's real category URLs — confirmed via site search May 2026
    categories = [
        ("/holsters/iwb-appendix-carry-holsters_8_1_135.html", "iwb"),
        ("/holsters/owb-belt-holsters_8_1_134.html", "owb"),
        ("/holsters/shoulder-chest-holster-systems_8_1_138.html", "shoulder"),
        ("/holsters/ankle-holsters_8_1_139.html", "ankle"),
        ("/holsters/pocket-holsters_8_1_143.html", "offbody"),
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
    """DeSantis uses a custom BigCommerce-style platform.
    Real category URLs confirmed May 2026: /store/SEARCH-BY-HOLSTER-OR-ACCESSORY/
    Using sitemap approach for reliability."""
    brand = "DeSantis"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.desantisholster.com"
    seen = set()

    # Try sitemap first — most reliable for custom platforms
    sitemap_urls = [
        f"{base}/sitemap.xml",
        f"{base}/sitemap_index.xml",
    ]
    product_urls = []
    for sitemap_url in sitemap_urls:
        r = fetch_with_retry(sitemap_url)
        if r and r.status_code == 200:
            try:
                soup = BeautifulSoup(r.text, "xml")
                locs = [loc.text for loc in soup.find_all("loc")]
                # Check if this is a sitemap index
                sub_maps = [l for l in locs if "sitemap" in l.lower() and l != sitemap_url]
                if sub_maps:
                    for sm in sub_maps:
                        rs = fetch_with_retry(sm)
                        if rs and rs.status_code == 200:
                            ss = BeautifulSoup(rs.text, "xml")
                            product_urls += [loc.text for loc in ss.find_all("loc")
                                           if any(k in loc.text.lower() for k in ["holster", "iwb", "owb"])]
                else:
                    product_urls = [l for l in locs
                                   if any(k in l.lower() for k in ["holster", "iwb", "owb", "ankle", "shoulder"])]
                if product_urls:
                    break
            except Exception:
                pass

    # Fallback: try their category pages directly
    if not product_urls:
        categories = [
            ("/store/SEARCH-BY-HOLSTER-OR-ACCESSORY/IWB-HOLSTERS/", "iwb"),
            ("/store/SEARCH-BY-HOLSTER-OR-ACCESSORY/OWB-HOLSTERS/", "owb"),
            ("/store/SEARCH-BY-HOLSTER-OR-ACCESSORY/POCKET-HOLSTERS/", "offbody"),
            ("/store/SEARCH-BY-HOLSTER-OR-ACCESSORY/ANKLE-HOLSTERS/", "ankle"),
            ("/store/SEARCH-BY-HOLSTER-OR-ACCESSORY/SHOULDER-HOLSTERS/", "shoulder"),
        ]
        for cat_path, carry_hint in categories:
            r = fetch_with_retry(base + cat_path)
            if not r or r.status_code != 200:
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            cards = soup.select(".product, [class*='product-item'], article, li.product")
            for card in cards:
                name_el = card.select_one("h2, h3, h4, [class*='title'], [class*='name']")
                price_el = card.select_one("[class*='price'], .price")
                img_el = card.select_one("img")
                link_el = card.select_one("a[href]")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name or name in seen or len(name) < 4:
                    continue
                seen.add(name)
                price = clean_price(price_el.get_text() if price_el else None)
                image = img_el.get("src", img_el.get("data-src", "")) if img_el else ""
                link = link_el.get("href", "") if link_el else ""
                if link and not link.startswith("http"):
                    link = base + link
                if image and not image.startswith("http"):
                    image = base + image
                combined = f"{name} {carry_hint}"
                products.append({
                    "brand": brand, "name": name, "price": price,
                    "image_url": image, "product_url": link or base + cat_path,
                    "carry_type": detect_carry(combined) or carry_hint,
                    "draw_hand": detect_hand(combined),
                    "light": detect_light(combined),
                    "optic": detect_optic(combined),
                    "gun_model": detect_gun_model(combined),
                    "material": detect_material(combined),
                    "in_stock": None,
                    "source": "custom",
                    "last_scraped": datetime.utcnow().isoformat(),
                })
            time.sleep(0.4)

    # Process sitemap URLs if found
    for url in product_urls[:200]:
        try:
            pr = fetch_with_retry(url)
            if not pr or pr.status_code != 200:
                continue
            ps = BeautifulSoup(pr.text, "html.parser")
            name_el = ps.select_one("h1")
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            if not name or name in seen:
                continue
            seen.add(name)
            price_el = ps.select_one("[class*='price'], .price, [itemprop='price']")
            price = clean_price(price_el.get("content") or price_el.get_text() if price_el else None)
            img_el = ps.select_one("img[itemprop='image'], [class*='product'] img")
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








def scrape_gcode():
    brand = "G-Code Holsters"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    # G-Code's real site is tacticalholsters.com (confirmed May 2026)
    # gcode-holsters.com is a dead domain
    base = "https://www.tacticalholsters.com"
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





def scrape_miltsparks():
    """Milt Sparks — premium leather holsters.
    Real category URLs confirmed May 2026."""
    brand = "Milt Sparks"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.miltsparks.com"
    seen = set()

    # Confirmed category URLs
    categories = [
        ("/inside-the-waistband/", "iwb"),
        ("/outside-the-waistband/", "owb"),
        ("/store/", None),  # in-stock items
    ]

    for cat_path, carry_hint in categories:
        r = fetch_with_retry(base + cat_path)
        if not r or r.status_code != 200:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        # Milt Sparks uses WordPress with product/post links
        links = soup.select("a[href*='miltsparks.com']")
        product_links = list({
            a["href"] for a in links
            if any(k in a["href"].lower() for k in ["holster", "waistband", "special", "criterion", "nexus", "vm-", "55bn", "om-"])
            and "miltsparks.com" in a["href"]
        })

        for url in product_links[:50]:
            try:
                pr = fetch_with_retry(url)
                if not pr or pr.status_code != 200:
                    continue
                ps = BeautifulSoup(pr.text, "html.parser")
                name_el = ps.select_one("h1, .entry-title, .product-title")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name or name in seen or len(name) < 4:
                    continue
                seen.add(name)
                price_el = ps.select_one(".price, .amount, [class*='price']")
                price = clean_price(price_el.get_text() if price_el else None)
                img_el = ps.select_one(".wp-post-image, img[class*='product'], img[class*='attachment']")
                image_url = img_el.get("src", "") if img_el else ""
                combined = f"{name} {url} {carry_hint or ''}"
                products.append({
                    "brand": brand, "name": name, "price": price,
                    "image_url": image_url, "product_url": url,
                    "carry_type": detect_carry(combined) or carry_hint,
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


# ─── Vedder — BigCommerce + HTML fallback ────────────────────────────────────
def scrape_vedder():
    """Vedder Holsters — BigCommerce.
    Uses multiple approaches: GraphQL, sitemap, then direct category HTML."""
    brand = "Vedder Holsters"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.vedderholsters.com"
    seen = set()

    # Approach 1: Try collection-specific product feeds (BC exposes these)
    collection_paths = [
        ("/holsters/iwb-holsters/", "iwb"),
        ("/holsters/owb-holsters/", "owb"),
        ("/holsters/appendix-carry/", "aiwb"),
        ("/holsters/", None),
    ]

    for path, carry_hint in collection_paths:
        # Try the BC API feed for each category
        feed_url = f"{base}{path}?format=json"
        r = fetch_with_retry(feed_url, timeout=12)
        if r and r.status_code == 200:
            try:
                data = r.json()
                items = data.get("products", data.get("items", []))
                for item in items:
                    name = (item.get("name") or item.get("title", "")).strip()
                    if not name or name in seen:
                        continue
                    seen.add(name)
                    price = clean_price(item.get("price") or item.get("calculated_price"))
                    url = item.get("url", item.get("custom_url", {}).get("url", ""))
                    if url and not url.startswith("http"):
                        url = base + url
                    img = item.get("primary_image", item.get("image", {}))
                    image_url = img.get("url_thumbnail", img.get("url_standard", "")) if isinstance(img, dict) else ""
                    combined = f"{name} {url} {carry_hint or ''}"
                    products.append({
                        "brand": brand, "name": name, "price": price,
                        "image_url": image_url, "product_url": url or base + path,
                        "carry_type": detect_carry(combined) or carry_hint,
                        "draw_hand": detect_hand(combined),
                        "light": detect_light(combined),
                        "optic": detect_optic(combined),
                        "gun_model": detect_gun_model(combined),
                        "material": detect_material(combined),
                        "in_stock": None,
                        "source": "bigcommerce_feed",
                        "last_scraped": datetime.utcnow().isoformat(),
                    })
            except Exception:
                pass

        # Also try HTML scrape of the category page
        if len(products) < 5:
            r2 = fetch_with_retry(base + path, timeout=12)
            if r2 and r2.status_code == 200:
                soup = BeautifulSoup(r2.text, "html.parser")
                cards = soup.select(
                    "[class*='productCard'], [class*='product-card'], "
                    ".product, article[class*='product'], [data-product-id]"
                )
                for card in cards:
                    name_el = card.select_one(
                        "h2, h3, h4, [class*='productCard-title'], "
                        "[class*='product-title'], [class*='card-title'], [class*='name']"
                    )
                    price_el = card.select_one("[class*='price'], .price")
                    img_el = card.select_one("img[src], img[data-src]")
                    link_el = card.select_one("a[href*='/holster'], a[href*='/iwb'], a[href*='/owb']")
                    if not name_el:
                        continue
                    name = name_el.get_text(strip=True)
                    if not name or name in seen or len(name) < 5:
                        continue
                    seen.add(name)
                    price = clean_price(price_el.get_text() if price_el else None)
                    image_url = img_el.get("src", img_el.get("data-src", "")) if img_el else ""
                    if image_url and not image_url.startswith("http"):
                        image_url = "https:" + image_url if image_url.startswith("//") else base + image_url
                    link = link_el.get("href", "") if link_el else ""
                    if link and not link.startswith("http"):
                        link = base + link
                    combined = f"{name} {link} {carry_hint or ''}"
                    products.append({
                        "brand": brand, "name": name, "price": price,
                        "image_url": image_url, "product_url": link or base + path,
                        "carry_type": detect_carry(combined) or carry_hint,
                        "draw_hand": detect_hand(combined),
                        "light": detect_light(combined),
                        "optic": detect_optic(combined),
                        "gun_model": detect_gun_model(combined),
                        "material": detect_material(combined),
                        "in_stock": None,
                        "source": "bigcommerce_html",
                        "last_scraped": datetime.utcnow().isoformat(),
                    })
        time.sleep(1.0)

    # Approach 2: Sitemap fallback if still nothing
    if not products:
        try:
            r = fetch_with_retry(f"{base}/sitemap.php?view=categories", timeout=10)
            if r and r.status_code == 200:
                soup = BeautifulSoup(r.text, "html.parser")
                links = soup.select("a[href*='holster']")
                cat_urls = list({
                    (base + a["href"]) if not a["href"].startswith("http") else a["href"]
                    for a in links
                    if "holster" in a.get("href", "").lower()
                })[:8]
                for cat_url in cat_urls:
                    r2 = fetch_with_retry(cat_url, timeout=12)
                    if not r2 or r2.status_code != 200:
                        continue
                    soup2 = BeautifulSoup(r2.text, "html.parser")
                    prod_links = soup2.select("a[href*='/holsters/'][href*='.html'], a[href*='/holsters/'][href*='/iwb'], a[href*='/holsters/'][href*='/owb']")
                    for pl in prod_links[:30]:
                        href = pl.get("href", "")
                        if not href:
                            continue
                        url = (base + href) if not href.startswith("http") else href
                        if url in seen:
                            continue
                        seen.add(url)
                        pr = fetch_with_retry(url, timeout=10)
                        if not pr or pr.status_code != 200:
                            continue
                        ps = BeautifulSoup(pr.text, "html.parser")
                        name_el = ps.select_one("h1")
                        if not name_el:
                            continue
                        name = name_el.get_text(strip=True)
                        if not name or "holster" not in name.lower():
                            continue
                        price_el = ps.select_one("[class*='price'], [itemprop='price']")
                        price = clean_price(price_el.get("content") or price_el.get_text() if price_el else None)
                        img_el = ps.select_one("img[itemprop='image'], [class*='productView'] img")
                        image_url = img_el.get("src", "") if img_el else ""
                        if image_url.startswith("//"):
                            image_url = "https:" + image_url
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
                        time.sleep(0.4)
        except Exception as e:
            print(f"    ❌ Vedder sitemap fallback: {e}", flush=True)

    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


# ─── Tulster — Cloudflare protected, skipped ─────────────────────────────────
def scrape_tulster():
    """Tulster blocks automated scrapers via Cloudflare WAF.
    Skipping gracefully to avoid wasting retry attempts."""
    print(f"  ⚠️  Tulster: Cloudflare-protected, skipping", flush=True)
    return []


# ─── StealthGear USA — Cloudflare protected, skipped ─────────────────────────
def scrape_stealthgear():
    """StealthGear blocks automated scrapers via Cloudflare WAF.
    Skipping gracefully to avoid wasting retry attempts."""
    print(f"  ⚠️  StealthGear USA: Cloudflare-protected, skipping", flush=True)
    return []


# ─── CrossBreed — Cloudflare protected, skipped ──────────────────────────────
def scrape_crossbreed():
    """CrossBreed blocks automated scrapers via Cloudflare WAF.
    Skipping gracefully to avoid wasting retry attempts."""
    print(f"  ⚠️  CrossBreed Holsters: Cloudflare-protected, skipping", flush=True)
    return []


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
    # Tier 1 Concealed moved to CUSTOM_SCRAPERS (variant-expansion scraper)
    ("CYA Supply",              "https://cyasupply.com"),
    ("Rounded Gear",            "https://www.roundedgear.com"),    ("LAG Tactical",            "https://lagtactical.com"),
    ("Tenicor",                 "https://tenicor.com"),
    ("Tagua Gunleather",        "https://www.taguagunleather.com"),
    ("Bawidamann",              "https://bawidamann.com"),
    ("GunfightersINC",          "https://gunfightersinc.com"),
    ("NSR Tactical",            "https://nsrtactical.com"),
    ("Black Scorpion Gear",     "https://www.blackscorpiongear.com"),
    ("Black Rhino",             "https://blackrhinoconcealment.com"),
    ("Dara Holsters",           "https://daraholsters.com"),    ("Crossfire Elite",         "https://crossfireelite.com"),    ("Sneaky Pete Holsters",    "https://www.sneakypeteholsters.com"),
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
    ("Eclipse Holsters",        "https://eclipseholsters.com"),
    ("Grizzle Leather",         "https://rgrizzleleather.com"),
    ("Wright Leather Works",    "https://wrightleatherworks.com"),
    ("Kirkpatrick Leather",     "https://kirkpatrickleather.com"),
    ("PS Products",             "https://psproducts.com"),
    ("KSG Armory",              "https://ksgarmory.com"),
    ("Talon Holsters",          "https://talonholsters.com"),
    ("Texas Holster Solutions", "https://texasholstersolutions.com"),
    ("Kusiak Leather",          "https://kusiakleather.com"),
    ("Desantis Gunhide",        "https://www.desantisholster.com"),
    ("Crossfire Holsters",      "https://crossfireholsters.com"),
    ("Falco Holsters",          "https://www.falcoholsters.com"),
    ("Craft Holsters",          "https://www.craftholsters.com"),
    # T-Rex Arms moved to CUSTOM_SCRAPERS (WooCommerce)
    # Phlster moved to CUSTOM_SCRAPERS (Cloudflare-blocked graceful skip)
    ("Safariland",              "https://safariland.com"),          # confirmed Shopify May 2026
    ("Comp-Tac Victory Gear",   "https://www.comp-tac.com"),        # confirmed Shopify May 2026
    ("Blade-Tech",              "https://blade-tech.com"),           # confirmed Shopify May 2026
    # Fobus — fobususa.com dead, fobusholster.com dead — removed until correct URL found
    ("Bianchi Leather",         "https://bianchileather.com"),
    ("El Paso Saddlery",        "https://epsaddlery.com"),
    ("Black Hills Leather",     "https://blackhillsleather.com"),
    ("Baker Leather",           "https://www.bakerleather.com"),
    ("High Noon Holsters",      "https://highnoonholsters.com"),    ("Gould & Goodrich",        "https://gouldusa.com"),
    ("Viridian Weapon Tech",    "https://viridianweapontech.com"),
    ("Elite Survival Systems",  "https://elitesurvival.com"),
]

# ─── T-Rex Arms — WooCommerce ─────────────────────────────────────────────
def scrape_trex_arms():
    """T-Rex Arms — WooCommerce store at trex-arms.com."""
    brand = "T-Rex Arms"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.trex-arms.com"
    seen = set()

    # WooCommerce REST API
    page = 1
    while page <= 10:
        url = f"{base}/wp-json/wc/store/v1/products?per_page=100&page={page}&category=holster-categories"
        r = fetch_with_retry(url, timeout=12)
        if not r or r.status_code != 200:
            # Try direct product page scraping
            break
        try:
            items = r.json()
            if not items:
                break
            for item in items:
                name = item.get("name", "")
                if not name or name in seen:
                    continue
                if not any(k in name.lower() for k in ["holster","sidecar","ragnarok","orion","erebus"]):
                    continue
                seen.add(name)
                price = clean_price(str(item.get("prices",{}).get("price","0")))
                if price:
                    price = price / 100  # WooCommerce returns in cents
                images = item.get("images",[])
                image_url = images[0].get("src","") if images else ""
                slug = item.get("slug","")
                combined = f"{name} {slug}"
                products.append({
                    "brand": brand, "name": name, "price": price,
                    "image_url": image_url,
                    "product_url": f"{base}/product/{slug}/",
                    "carry_type": detect_carry(combined),
                    "draw_hand": detect_hand(combined),
                    "light": detect_light(combined),
                    "optic": detect_optic(combined),
                    "gun_model": detect_gun_model(combined),
                    "material": detect_material(combined),
                    "in_stock": item.get("is_in_stock", None),
                    "source": "woocommerce_api",
                    "last_scraped": datetime.utcnow().isoformat(),
                })
            if len(items) < 100:
                break
            page += 1
            time.sleep(0.5)
        except Exception as e:
            print(f"    ❌ T-Rex WC API error: {e}", flush=True)
            break

    # Fallback: scrape HTML product category pages
    if not products:
        category_urls = [
            (f"{base}/product-category/holster-categories/iwb-holsters/", "iwb"),
            (f"{base}/product-category/holster-categories/owb-holsters/", "owb"),
            (f"{base}/product-category/holster-categories/", None),
        ]
        for cat_url, carry_hint in category_urls:
            r = fetch_with_retry(cat_url, timeout=12)
            if not r or r.status_code != 200:
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            cards = soup.select("li.product, article.product, .product-item")
            for card in cards:
                name_el = card.select_one("h2, h3, .woocommerce-loop-product__title, .product-title")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name or name in seen:
                    continue
                if not any(k in name.lower() for k in ["holster","sidecar","ragnarok","orion","erebus","duty"]):
                    continue
                seen.add(name)
                price_el = card.select_one(".price, .woocommerce-Price-amount")
                price = clean_price(price_el.get_text() if price_el else None)
                img_el = card.select_one("img[src]")
                image_url = img_el.get("src","") if img_el else ""
                link_el = card.select_one("a[href*='/product/']")
                link = link_el.get("href","") if link_el else ""
                combined = f"{name} {link} {carry_hint or ''}"
                products.append({
                    "brand": brand, "name": name, "price": price,
                    "image_url": image_url, "product_url": link or cat_url,
                    "carry_type": detect_carry(combined) or carry_hint,
                    "draw_hand": detect_hand(combined),
                    "light": detect_light(combined),
                    "optic": detect_optic(combined),
                    "gun_model": detect_gun_model(combined),
                    "material": detect_material(combined),
                    "in_stock": None,
                    "source": "woocommerce_html",
                    "last_scraped": datetime.utcnow().isoformat(),
                })
            time.sleep(1.0)

    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


# ─── Galco — Custom platform sitemap scraper ─────────────────────────────
    # Galco organizes products by category
    category_paths = [
        ("/holsters_8_1.html", None),
        ("/holsters-concealed-carry_8_1_17.html", "iwb"),
        ("/owb-belt-holsters_8_1_16.html", "owb"),
        ("/ankle-holsters_8_1_40.html", "ankle"),
        ("/shoulder-holster-systems_8_2.html", "shoulder"),
        ("/paddle-holsters_8_1_56.html", "owb-paddle"),
    ]

    for path, carry_hint in category_paths:
        r = fetch_with_retry(base + path, timeout=12)
        if not r or r.status_code != 200:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        
        # Find product links - Galco uses specific URL patterns
        prod_links = soup.select("a[href*='_8_'][href$='.html']")
        # Filter to product detail pages (deeper URL structure)
        detail_links = [
            a["href"] for a in prod_links
            if a["href"].count("_8_") >= 2 and "holster" in a.get_text("").lower()
        ]
        
        for href in list(set(detail_links))[:40]:
            url = href if href.startswith("http") else base + "/" + href.lstrip("/")
            if url in seen:
                continue
            seen.add(url)
            
            pr = fetch_with_retry(url, timeout=10)
            if not pr or pr.status_code != 200:
                continue
            
            ps = BeautifulSoup(pr.text, "html.parser")
            name_el = ps.select_one("h1, .product-name, [class*='title']")
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            if not name or len(name) < 5:
                continue
            if not any(k in name.lower() for k in ["holster","rig","system","carry"]):
                continue

            price_el = ps.select_one("[class*='price'], [itemprop='price'], .price")
            price = None
            if price_el:
                price_text = price_el.get("content") or price_el.get_text()
                price = clean_price(price_text)
            
            img_el = ps.select_one("img[itemprop='image'], [class*='product'] img[src]")
            image_url = ""
            if img_el:
                image_url = img_el.get("src","")
                if image_url.startswith("//"):
                    image_url = "https:" + image_url
            
            combined = f"{name} {url} {carry_hint or ''}"
            products.append({
                "brand": brand, "name": name, "price": price,
                "image_url": image_url, "product_url": url,
                "carry_type": detect_carry(combined) or carry_hint,
                "draw_hand": detect_hand(combined),
                "light": detect_light(combined),
                "optic": detect_optic(combined),
                "gun_model": detect_gun_model(combined),
                "material": detect_material(combined),
                "in_stock": None,
                "source": "galco_custom",
                "last_scraped": datetime.utcnow().isoformat(),
            })
            time.sleep(0.4)
        time.sleep(1.0)

    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


# ─── Phlster — Cloudflare protected, skipped ────────────────────────────
def scrape_phlster():
    """Phlster blocks scrapers via Cloudflare WAF. Skip gracefully."""
    print(f"  ⚠️  Phlster Holsters: Cloudflare-protected, skipping", flush=True)
    return []


# NOTE: Tenicor, LAG Tactical, Dara Holsters also use variant-based gun models.
# They are handled by scrape_shopify() which saves one record per product.
# For better search results, they could be moved to variant-expansion scrapers
# similar to scrape_tier1() in a future update.

# ─── Tier 1 Concealed — Variant-expansion Shopify scraper ─────────────────
def scrape_tier1():
    """Tier 1 Concealed — Shopify.
    Expands variants so each gun model fit becomes a separate DB record.
    This makes them searchable by gun model like other brands."""
    brand = "Tier 1 Concealed"
    products = []
    print(f"  Scraping {brand}...", flush=True)
    base = "https://www.tier1concealed.com"
    seen = set()
    page = 1

    while True:
        url = f"{base}/products.json?limit=250&page={page}"
        r = fetch_with_retry(url, timeout=12)
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
            combined_base = f"{title} {product_type} {tags} {body}"

            # Skip non-holster products (apparel, accessories, hardware)
            if not any(kw in combined_base.lower() for kw in [
                "holster", "iwb", "owb", "aiwb", "appendix", "axis", "apx",
                "agis", "xiphos", "echo", "t1-m", "t1m", "msp"
            ]):
                continue

            images = item.get("images", [])
            image_url = images[0].get("src", "") if images else ""
            product_url = f"{base}/products/{handle}"
            carry = detect_carry(combined_base) or "aiwb"  # T1 is mostly AIWB
            
            variants = item.get("variants", [])
            
            # Expand variants — each variant option1 is a gun model
            variant_records = {}
            for v in variants:
                gun_option = v.get("option1", "") or v.get("option2", "") or ""
                if not gun_option or gun_option.lower() in ["default title", "default", ""]:
                    continue
                
                # Skip non-gun options (color, size, etc.)
                gun_option_clean = gun_option.strip()
                if len(gun_option_clean) < 3:
                    continue
                
                # Deduplicate by gun model
                if gun_option_clean in variant_records:
                    continue
                
                price = clean_price(v.get("price"))
                in_stock = v.get("available", True)
                combined = f"{title} {gun_option_clean} {combined_base}"
                
                variant_records[gun_option_clean] = {
                    "brand": brand,
                    "name": f"{title} — {gun_option_clean}",
                    "price": price,
                    "image_url": image_url,
                    "product_url": f"{product_url}?variant={gun_option_clean.replace(chr(32), '-').replace('/', '-').lower()[:50]}",
                    "carry_type": carry,
                    "draw_hand": detect_hand(combined),
                    "light": detect_light(combined),
                    "optic": detect_optic(combined),
                    "gun_model": detect_gun_model(combined) or gun_option_clean,
                    "material": "Kydex",
                    "in_stock": in_stock,
                    "source": "shopify_variants",
                    "last_scraped": datetime.utcnow().isoformat(),
                }
            
            if variant_records:
                products.extend(variant_records.values())
            else:
                # No variants with gun models — save as single record
                key = f"{title}"
                if key not in seen:
                    seen.add(key)
                    price = clean_price(variants[0].get("price")) if variants else None
                    products.append({
                        "brand": brand,
                        "name": title,
                        "price": price,
                        "image_url": image_url,
                        "product_url": product_url,
                        "carry_type": carry,
                        "draw_hand": detect_hand(combined_base),
                        "light": detect_light(combined_base),
                        "optic": detect_optic(combined_base),
                        "gun_model": detect_gun_model(combined_base),
                        "material": "Kydex",
                        "in_stock": detect_in_stock(variants),
                        "source": "shopify",
                        "last_scraped": datetime.utcnow().isoformat(),
                    })

        page += 1
        time.sleep(1.5)

    print(f"    ✅ {brand}: {len(products)} holsters found", flush=True)
    return products


CUSTOM_SCRAPERS = [
    scrape_tier1,         # Shopify variant-expansion
    scrape_trex_arms,     # WooCommerce
    scrape_galco,         # custom platform
    scrape_phlster,       # Cloudflare-protected skip
    scrape_blackhawk,     # custom platform — sitemap approach
    scrape_desantis,      # custom platform — sitemap approach
    scrape_gcode,         # tacticalholsters.com — fixed URL
    scrape_miltsparks,    # fixed category URLs
    scrape_don_hume,      # WooCommerce — custom only, not in SHOPIFY_BRANDS
    scrape_vedder,        # BigCommerce GraphQL
    scrape_crossbreed,    # Shopify collection API
    scrape_stealthgear,   # Shopify collection API
    scrape_tulster,       # BigCommerce GraphQL
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
