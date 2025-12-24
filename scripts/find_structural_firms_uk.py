import os
import re
import time
import math
import requests
from urllib.parse import urlparse, urljoin
from openpyxl import Workbook

# ---------- Config (from GitHub Secrets/Vars) ----------
API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
HOME_POSTCODE = os.getenv("HOME_POSTCODE", "M1 1AE")  # default: Manchester (change!)
RADIUS_M = int(os.getenv("SEARCH_RADIUS_M", "25000"))
QUERY = os.getenv("SEARCH_QUERY", "structural engineering consultants")

if not API_KEY:
    raise SystemExit("Missing GOOGLE_MAPS_API_KEY (add it as a GitHub repo secret).")

UA = {"User-Agent": "FBC-CompanyFinder/1.0 (respect robots and rate limits)"}
TIMEOUT = 25

# Only accept role-based emails (avoid personal email harvesting)
GENERIC_EMAIL = re.compile(
    r"\b(careers|recruitment|recruiting|jobs|talent|people|hiring|vacancies|hr)"
    r"@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
    re.IGNORECASE
)

# Candidate pages to check ON THE COMPANY WEBSITE ONLY
CANDIDATE_PATHS = [
    "/", "/careers", "/careers/", "/jobs", "/jobs/", "/join-us", "/join-us/",
    "/work-with-us", "/work-with-us/", "/contact", "/contact/", "/about", "/about/"
]

def http_get(url: str, params=None):
    r = requests.get(url, params=params, headers=UA, timeout=TIMEOUT)
    r.raise_for_status()
    return r

# ---------- Google APIs ----------
def geocode_postcode(postcode: str):
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    data = http_get(url, params={"address": postcode, "key": API_KEY}).json()
    if data.get("status") != "OK" or not data.get("results"):
        raise SystemExit(f"Geocode failed for postcode '{postcode}': {data.get('status')}")
    loc = data["results"][0]["geometry"]["location"]
    return float(loc["lat"]), float(loc["lng"])

def places_text_search(query: str, lat: float, lng: float, radius_m: int, pagelimit=3):
    """
    Uses Places Text Search, location biased by radius.
    Returns list of place results (name, place_id, formatted_address, etc.)
    """
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    results = []
    page_token = None

    for _ in range(pagelimit):
        params = {
            "query": query,
            "location": f"{lat},{lng}",
            "radius": radius_m,
            "key": API_KEY
        }
        if page_token:
            params["pagetoken"] = page_token

        data = http_get(url, params=params).json()
        status = data.get("status")

        if status not in ("OK", "ZERO_RESULTS"):
            # OVER_QUERY_LIMIT, REQUEST_DENIED, INVALID_REQUEST etc.
            raise SystemExit(f"Places search error: {status} - {data.get('error_message')}")

        results.extend(data.get("results", []))
        page_token = data.get("next_page_token")
        if not page_token:
            break

        # Google requires a short wait before next_page_token becomes valid
        time.sleep(2.2)

    return results

def place_details(place_id: str):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    fields = "name,website,formatted_address,international_phone_number,url"
    data = http_get(url, params={"place_id": place_id, "fields": fields, "key": API_KEY}).json()
    if data.get("status") != "OK":
        return None
    return data.get("result", {})

# ---------- Website email extraction (company site only) ----------
def normalize_base(website: str):
    if not website:
        return ""
    p = urlparse(website)
    if not p.scheme:
        website = "https://" + website
        p = urlparse(website)
    return f"{p.scheme}://{p.netloc}"

def same_domain(url: str, base: str) -> bool:
    return urlparse(url).netloc.lower() == urlparse(base).netloc.lower()

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

        time.sleep(0.5)

    return sorted(found), checked

# ---------- Distance (rough) ----------
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    p1 = math.radians(lat1); p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlon/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def main():
    lat, lng = geocode_postcode(HOME_POSTCODE)

    # Search
    places = places_text_search(QUERY, lat, lng, RADIUS_M, pagelimit=3)

    # De-dup by place_id
    seen = set()
    rows = []

    for p in places:
        pid = p.get("place_id")
        if not pid or pid in seen:
            continue
        seen.add(pid)

        det = place_details(pid)
        if not det:
            continue

        name = det.get("name", "")
        website = det.get("website", "")
        address = det.get("formatted_address", "")
        phone = det.get("international_phone_number", "")
        maps_url = det.get("url", "")

        base = normalize_base(website)
        emails = []
        checked = []

        if base:
            emails, checked = extract_generic_emails_from_site(base)

        # add distance if geometry present in search results
        dist_km = ""
        try:
            gloc = p["geometry"]["location"]
            dist_km = round(haversine_km(lat, lng, gloc["lat"], gloc["lng"]), 1)
        except Exception:
            pass

        rows.append({
            "company_name": name,
            "distance_km": dist_km,
            "address": address,
            "phone": phone,
            "website": website,
            "generic_recruitment_emails_on_website": " | ".join(emails),
            "pages_checked": " | ".join(checked[:6]),
            "google_maps_place_url": maps_url,
        })

    # Sort: closest first (blank distances last)
    def sort_key(r):
        return (9999 if r["distance_km"] == "" else r["distance_km"], r["company_name"].lower())
    rows.sort(key=sort_key)

    # Write Excel
    os.makedirs("out", exist_ok=True)
    wb = Workbook()
    ws = wb.active
    ws.title = "UK Firms Near Me"

    headers = list(rows[0].keys()) if rows else [
        "company_name","distance_km","address","phone","website",
        "generic_recruitment_emails_on_website","pages_checked","google_maps_place_url"
    ]
    ws.append(headers)
    for r in rows:
        ws.append([r.get(h, "") for h in headers])

    out_path = "out/uk_structural_companies_near_me.xlsx"
    wb.save(out_path)
    print(f"Saved {len(rows)} rows -> {out_path}")

if __name__ == "__main__":
    main()
