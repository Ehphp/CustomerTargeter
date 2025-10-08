-- Normalizza a partire da places_raw per rispettare la FK e lo staging
INSERT INTO places_clean(
  place_id, name, address, city, category, rating, user_ratings_total,
  hours_weekly, has_phone, has_website, location, istat_code
)
SELECT
  pr.place_id,
  COALESCE(pr.name, CONCAT(upper(COALESCE(pr.types[2], pr.types[1])),' (OSM)')) AS name,
  pr.formatted_address,
  NULL AS city,
  COALESCE(pr.types[2], pr.types[1]) AS category, -- preferisci subtype, fallback a category
  pr.rating,
  COALESCE(pr.user_ratings_total, 0),
  CASE WHEN pr.opening_hours_json IS NOT NULL THEN 60 ELSE 0 END AS hours_weekly,
  (pr.phone IS NOT NULL) AS has_phone,
  (pr.website IS NOT NULL) AS has_website,
  pr.location,
  NULL AS istat_code
FROM places_raw pr
ON CONFLICT(place_id) DO UPDATE SET
  name=EXCLUDED.name,
  category=EXCLUDED.category,
  hours_weekly=EXCLUDED.hours_weekly,
  has_phone=EXCLUDED.has_phone,
  has_website=EXCLUDED.has_website,
  location=EXCLUDED.location;
