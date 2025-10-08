-- Normalizza a partire da places_raw per rispettare la FK e lo staging
WITH base AS (
  SELECT
    pr.*,
    ob.tags
  FROM places_raw pr
  LEFT JOIN osm_business ob
    ON pr.place_id = CONCAT('OSM_', ob.osm_id)
),
enriched AS (
  SELECT
    b.place_id,
    b.name,
    b.formatted_address,
    b.types,
    b.rating,
    COALESCE(b.user_ratings_total, 0) AS user_ratings_total,
    b.opening_hours_json,
    b.phone,
    b.website,
    b.location,
    COALESCE(
      NULLIF(
        TRIM(
          COALESCE(
            b.tags ->> 'addr:city',
            b.tags ->> 'addr:town',
            b.tags ->> 'addr:village',
            b.tags ->> 'addr:hamlet'
          )
        ),
        ''
      ),
      ic.comune
    ) AS city,
    ic.istat_code
  FROM base b
  LEFT JOIN LATERAL (
    SELECT i.istat_code, i.comune
    FROM istat_comuni i
    WHERE i.geom && b.location::geometry
      AND ST_Intersects(i.geom, b.location::geometry)
    ORDER BY i.popolazione DESC NULLS LAST, i.comune
    LIMIT 1
  ) ic ON TRUE
)
INSERT INTO places_clean(
  place_id, name, address, city, category, rating, user_ratings_total,
  hours_weekly, has_phone, has_website, location, istat_code
)
SELECT
  e.place_id,
  COALESCE(e.name, CONCAT(upper(COALESCE(e.types[2], e.types[1])),' (OSM)')) AS name,
  e.formatted_address,
  e.city,
  COALESCE(e.types[2], e.types[1]) AS category, -- preferisci subtype, fallback a category
  e.rating,
  e.user_ratings_total,
  CASE WHEN e.opening_hours_json IS NOT NULL THEN 60 ELSE 0 END AS hours_weekly,
  (e.phone IS NOT NULL) AS has_phone,
  (e.website IS NOT NULL) AS has_website,
  e.location,
  e.istat_code
FROM enriched e
ON CONFLICT(place_id) DO UPDATE SET
  name=EXCLUDED.name,
  address=EXCLUDED.address,
  city=EXCLUDED.city,
  category=EXCLUDED.category,
  rating=EXCLUDED.rating,
  user_ratings_total=EXCLUDED.user_ratings_total,
  hours_weekly=EXCLUDED.hours_weekly,
  has_phone=EXCLUDED.has_phone,
  has_website=EXCLUDED.has_website,
  location=EXCLUDED.location,
  istat_code=EXCLUDED.istat_code;
