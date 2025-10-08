import os, time, json, requests, psycopg2
from dotenv import load_dotenv
load_dotenv()

API = os.getenv("GOOGLE_PLACES_API_KEY")
PG = dict(host="localhost", port=os.getenv("POSTGRES_PORT","5432"),
          dbname=os.getenv("POSTGRES_DB"), user=os.getenv("POSTGRES_USER"),
          password=os.getenv("POSTGRES_PASSWORD"))

FIELDS = "place_id,name,formatted_address,formatted_phone_number,website,types,rating,user_ratings_total,opening_hours,geometry/location"

def text_search(query, lat, lng, radius=1500):
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {"key": API, "query": query, "location": f"{lat},{lng}", "radius": radius}
    while True:
        r = requests.get(url, params=params).json()
        for res in r.get("results", []):
            yield res["place_id"]
        tok = r.get("next_page_token")
        if not tok: break
        time.sleep(2); params["pagetoken"] = tok

def details(pid):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    return requests.get(url, params={"key": API, "place_id": pid, "fields": FIELDS}).json()["result"]

def upsert_place(conn, r):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO places_raw(place_id,name,formatted_address,phone,website,types,rating,user_ratings_total,opening_hours_json,location)
        VALUES (%(place_id)s,%(name)s,%(formatted_address)s,%(phone)s,%(website)s,%(types)s,%(rating)s,%(user_ratings_total)s,%(opening_hours_json)s,
                ST_SetSRID(ST_MakePoint(%(lng)s,%(lat)s),4326)::geography)
        ON CONFLICT (place_id) DO UPDATE SET
          name=EXCLUDED.name, formatted_address=EXCLUDED.formatted_address, phone=EXCLUDED.phone, website=EXCLUDED.website,
          types=EXCLUDED.types, rating=EXCLUDED.rating, user_ratings_total=EXCLUDED.user_ratings_total,
          opening_hours_json=EXCLUDED.opening_hours_json, location=EXCLUDED.location, source_ts=now();
        """, {
            "place_id": r["place_id"],
            "name": r.get("name"),
            "formatted_address": r.get("formatted_address"),
            "phone": r.get("formatted_phone_number"),
            "website": r.get("website"),
            "types": r.get("types"),
            "rating": r.get("rating"),
            "user_ratings_total": r.get("user_ratings_total"),
            "opening_hours_json": json.dumps(r.get("opening_hours")),
            "lat": r["geometry"]["location"]["lat"],
            "lng": r["geometry"]["location"]["lng"],
        })

def run(seed_lat, seed_lng):
    conn = psycopg2.connect(**PG); conn.autocommit = True
    try:
        # esempio semplice: tre query principali
        for q in ["ristorante", "bar", "negozio"]:
            for pid in text_search(q, seed_lat, seed_lng, radius=3000):
                upsert_place(conn, details(pid))
    finally:
        conn.close()

if __name__ == "__main__":
    # Esempio: centro città (metti coordinate della città pilota)
    run(41.729, 13.342)  # ← esempio Alatri
