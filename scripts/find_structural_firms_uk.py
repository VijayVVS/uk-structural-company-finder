import os
import re
import time
import math
import json
import requests
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin
from openpyxl import Workbook, load_workbook

# ---------- Config ----------
API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
HOME_POSTCODE = os.getenv("HOME_POSTCODE", "SK3 9AR")
RADIUS_M = int(os.getenv("SEARCH_RADIUS_M", "30000"))
QUERY = os.getenv("SEARCH_QUERY", "civil and structural engineers")
DAILY_LIMIT = int(os.getenv("DAILY_LIMIT", "25"))

if not API_KEY:
    raise SystemExit("Missing GOOGLE_MAPS_API_KEY (add it as a GitHub repo secret).")

UA = {"User-Agent": "FBC-CompanyFinder/1.0"}
TIMEOUT = 25

# Only accept role-based emails (avoid personal email harvesting)
GENERIC_EMAIL = re.compile(
    r"\b(careers|recruitment|recruiting|jobs|talent|people|hiring|vacancies|hr)"
    r"@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
    re.IGNORECASE
)

CANDIDATE_PATHS = [
    "/", "/careers", "/careers/", "/jobs", "/jobs/", "/join-us", "/join-us/",
    "/work-with-us", "/work-with-us/", "/contact", "/contact/", "/contact-us", "/contact-us/",
    "/vacancies", "/vacancies/", "/join", "/join/"
]

SEEN_PATH = "data/seen_companies.json"
MASTER_PATH = "data/master_companies.xlsx"   # optional running master
OUT_DAILY_DIR = "out"

def http_get(url: str, params=None):
    r = requests.get(url, params=params, headers=UA, timeout=TIMEOUT)
    r.raise_for_status()
    return r

# ---------- Google APIs ----------
def geocode_postcode(postcode: str):
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    data = http_get(url, params={"address": postcode, "key": API_KEY}).json()
    if data.get("status") != "OK" or not data.get("results"):
        raise SystemExit(f"Geocode failed for '{postcode}': {data.get('status')} {data.get('error_message')}")
    loc = data["results"][0]["geometry"]["location"]
    return float(loc["lat"]), float(loc["lng"])

def places_text_search(query: str, lat: float, lng: float, radius_m: int, pagelimit=3):
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    results = []
    page_token = None

    for _ in range(pagelimit):
        params = {"query": query, "location": f"{lat},{lng}", "radius": radius_m, "key": API_KEY}
        if page_token:
            params["pagetoken"] = page_token

        data = http_get(url, params=params).json()
        status = data.get("status")

        if status not in ("OK", "ZERO_RESULTS"):
            raise SystemExit(f"Places search error: {status} - {data.get('error_message')}")

        results.extend(data.get("results", []))
        page_token = data.get("next_page_token")
        if not page_token:
            break
        time.sleep(2.2)  # token warm-up

    return results

def place_details(place_id: str):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    fields = "name,website,formatted_address,international_phone_number,url"
    data = http_get(url, params={"place_id": place_id, "fields": fields, "key": API_KEY}).json()
    if data.get("status") != "OK":
        return None
    return data.get("result", {})

# ---------- Website email extraction ----------
def normalize_base(website: str):
    if not website:
        return ""
    p = urlparse(website)
    if not p.scheme:
        website = "https://" + website
        p = urlparse(website)
    return f"{p.scheme}://{p.netloc}"

def extract_generic_emails_from_site(base: str):
    found = set()
    checked = []

    for path in CANDIDATE_PATHS:
        url = urljoin(base, path)
        try:
            html = http_get(url).text
        except Exception:
            continue

        checked.append(url)
        for m in GENERIC_EMAIL.finditer(html):
            found.add(m.group(0))

        time.sleep(0.4)

    return sorted(found), checked

# ---------- Distance ----------
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    p1 = math.radians(lat1); p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlon/2)**2
    return 2 * R * math.asin(math.sqrt(a))

# ---------- Seen state ----------
def load_seen():
    os.makedirs(os.path.dirname(SEEN_PATH), exist_ok=True)
    if not os.path.exists(SEEN_PATH):
        return {"seen_place_ids": [], "seen_domains": []}
    with open(SEEN_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_seen(seen):
    with open(SEEN_PATH, "w", encoding="utf-8") as f:
        json.dump(seen, f, indent=2)

# ---------- Excel helpers ----------
def write_daily_excel(rows, out_path):
    wb = Workbook()
    ws = wb.active
    ws.title = "Daily 25"
    headers = [
        "run_date_utc",
        "company_name",
        "distance_km",
        "address",
        "phone",
        "website",
        "generic_recruitment_emails_on_website",
        "pages_checked",
        "google_maps_place_url",
        "place_id",
        "company_domain"
    ]
    ws.append(headers)
    for r in rows:
        ws.append([r.get(h, "") for h in headers])
    wb.save(out_path)

def append_to_master(rows):
    # keep a running master workbook (optional)
    os.makedirs(os.path.dirname(MASTER_PATH), exist_ok=True)
    headers = [
        "run_date_utc",
        "company_name",
        "distance_km",
        "address",
        "phone",
        "website",
        "generic_recruitment_emails_on_website",
        "pages_checked",
        "google_maps_place_url",
        "place_id",
        "company_domain"
    ]

    if not os.path.exists(MASTER_PATH):
        wb = Workbook()
        ws = wb.active
        ws.title = "Master"
        ws.append(headers)
        wb.save(MASTER_PATH)

    wb = load_workbook(MASTER_PATH)
    ws = wb["Master"]
    for r in rows:
        ws.append([r.get(h, "") for h in headers])
    wb.save(MASTER_PATH)

def main():
    seen = load_seen()
    seen_place_ids = set(seen.get("seen_place_ids", []))
    seen_domains = set(seen.get("seen_domains", []))

    lat, lng = geocode_postcode(HOME_POSTCODE)
    places = places_text_search(QUERY, lat, lng, RADIUS_M, pagelimit=3)

    # Sort by distance (closest first)
    enriched = []
    for p in places:
        try:
            gloc = p["geometry"]["location"]
            dist = haversine_km(lat, lng, gloc["lat"], gloc["lng"])
        except Exception:
            dist = 9999.0
        enriched.append((dist, p))
    enriched.sort(key=lambda x: x[0])

    selected_rows = []
    for dist, p in enriched:
        if len(selected_rows) >= DAILY_LIMIT:
            break

        pid = p.get("place_id")
        if not pid or pid in seen_place_ids:
            continue

        det = place_details(pid)
        if not det:
            continue

        website = det.get("website", "") or ""
        base = normalize_base(website)
        domain = urlparse(base).netloc.lower() if base else ""

        # Dedupe by domain too (helps when Maps has multiple listings)
        if domain and domain in seen_domains:
            # mark the place ID as seen as well to avoid revisiting
            seen_place_ids.add(pid)
            continue

        emails, checked = ([], [])
        if base:
            emails, checked = extract_generic_emails_from_site(base)

        row = {
            "run_date_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "company_name": det.get("name", ""),
            "distance_km": round(dist, 1) if dist < 9000 else "",
            "address": det.get("formatted_address", ""),
            "phone": det.get("international_phone_number", ""),
            "website": website,
            "generic_recruitment_emails_on_website": " | ".join(emails),
            "pages_checked": " | ".join(checked[:6]),
            "google_maps_place_url": det.get("url", ""),
            "place_id": pid,
            "company_domain": domain
        }

        selected_rows.append(row)

        # Update seen as soon as we accept it
        seen_place_ids.add(pid)
        if domain:
            seen_domains.add(domain)

        time.sleep(0.2)

    # Save seen state
    seen["seen_place_ids"] = sorted(seen_place_ids)
    seen["seen_domains"] = sorted(seen_domains)
    save_seen(seen)

    # Write outputs
    os.makedirs(OUT_DAILY_DIR, exist_ok=True)
    daily_name = f"uk_companies_{datetime.now(timezone.utc).strftime('%Y%m%d')}.xlsx"
    daily_path = os.path.join(OUT_DAILY_DIR, daily_name)
    write_daily_excel(selected_rows, daily_path)

    # Optional: keep a master workbook in /data
    append_to_master(selected_rows)

    print(f"Selected {len(selected_rows)} new companies (limit={DAILY_LIMIT}).")
    print(f"Daily Excel: {daily_path}")
    print(f"Master Excel: {MASTER_PATH}")
    print(f"Seen file updated: {SEEN_PATH}")

if __name__ == "__main__":
    main()
