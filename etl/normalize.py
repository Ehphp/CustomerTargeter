import psycopg2, os, json
from dotenv import load_dotenv
load_dotenv()
PG = dict(host="localhost", port=os.getenv("POSTGRES_PORT","5432"),
          dbname=os.getenv("POSTGRES_DB"), user=os.getenv("POSTGRES_USER"),
          password=os.getenv("POSTGRES_PASSWORD"))

def hours_per_week(oh):
    if not oh or "periods" not in oh: return 0
    tot=0
    for p in oh["periods"]:
        o=p.get("open"); c=p.get("close")
        if not o or not c: continue
        ot=int(o["time"][:2])*60+int(o["time"][2:])
        ct=int(c["time"][:2])*60+int(c["time"][2:])
        if ct>=ot: tot += (ct-ot)
    return round(tot/60)

def run():
    conn = psycopg2.connect(**PG); conn.autocommit=True
    with conn.cursor() as cur:
        cur.execute("SELECT place_id,name,formatted_address,phone,website,types,rating,user_ratings_total,opening_hours_json FROM places_raw;")
        rows = cur.fetchall()
        for (pid,name,addr,phone,site,types,rat,urt,oh_json) in rows:
            city = addr.split(",")[-2].strip() if addr and "," in addr else None
            category = (types[0] if types else None)
            hours = hours_per_week(oh_json)
            cur.execute("""
            INSERT INTO places_clean(place_id,name,address,city,category,rating,user_ratings_total,hours_weekly,has_phone,has_website,location,istat_code)
            SELECT pr.place_id,%s,%s,%s,%s,%s,%s,%s,%s,%s,pr.location,NULL
            FROM places_raw pr WHERE pr.place_id=%s
            ON CONFLICT(place_id) DO UPDATE SET
              name=EXCLUDED.name,address=EXCLUDED.address,city=EXCLUDED.city,category=EXCLUDED.category,
              rating=EXCLUDED.rating,user_ratings_total=EXCLUDED.user_ratings_total,hours_weekly=EXCLUDED.hours_weekly,
              has_phone=EXCLUDED.has_phone,has_website=EXCLUDED.has_website,location=EXCLUDED.location;
            """, (name, addr, city, category, rat, urt, hours, bool(phone), bool(site), pid))
    conn.close()

if __name__=="__main__":
    run()
