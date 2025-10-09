-- ============================================
-- normalize_places.sql
-- Normalizza i record provenienti da Google Places in places_clean.
-- ============================================

WITH base AS (
    SELECT
        pr.place_id,
        pr.name,
        pr.formatted_address,
        pr.types,
        pr.rating,
        COALESCE(pr.user_ratings_total, 0) AS user_ratings_total,
        pr.opening_hours_json,
        pr.phone,
        pr.website,
        pr.location,
        pr.source_ts
    FROM places_raw pr
),
address_parts AS (
    SELECT
        b.*,
        regexp_split_to_array(COALESCE(b.formatted_address, ''), ',') AS addr_parts
    FROM base b
),
enriched AS (
    SELECT
        a.place_id,
        a.name,
        a.formatted_address,
        a.addr_parts,
        a.types,
        a.rating,
        a.user_ratings_total,
        a.opening_hours_json,
        a.phone,
        a.website,
        a.location,
        a.source_ts,
        CASE
            WHEN array_length(a.addr_parts, 1) >= 2 THEN NULLIF(trim(a.addr_parts[array_length(a.addr_parts, 1) - 1]), '')
            WHEN array_length(a.addr_parts, 1) = 1 THEN NULLIF(trim(a.addr_parts[1]), '')
            ELSE NULL
        END AS formatted_city,
        ic.comune AS istat_city,
        ic.istat_code
    FROM address_parts a
    LEFT JOIN LATERAL (
        SELECT i.istat_code, i.comune
        FROM istat_comuni i
        WHERE i.geom && a.location::geometry
          AND ST_Intersects(i.geom, a.location::geometry)
        ORDER BY i.popolazione DESC NULLS LAST, i.comune
        LIMIT 1
    ) ic ON TRUE
)
INSERT INTO places_clean(
  place_id,
  name,
  address,
  city,
  category,
  rating,
  user_ratings_total,
  hours_weekly,
  has_phone,
  has_website,
  location,
  istat_code
)
SELECT
    e.place_id,
    COALESCE(e.name, INITCAP(COALESCE(e.types[1], e.types[2], 'Attivita'))) AS name,
    e.formatted_address,
    COALESCE(e.formatted_city, e.istat_city) AS city,
    CASE
        WHEN e.types IS NOT NULL AND array_length(e.types, 1) >= 1 THEN e.types[1]
        WHEN e.types IS NOT NULL AND array_length(e.types, 1) >= 2 THEN e.types[2]
        ELSE NULL
    END AS category,
    e.rating,
    e.user_ratings_total,
    CASE WHEN e.opening_hours_json IS NOT NULL THEN 60 ELSE 0 END AS hours_weekly,
    (e.phone IS NOT NULL) AS has_phone,
    (e.website IS NOT NULL) AS has_website,
    e.location,
    e.istat_code
FROM enriched e
ON CONFLICT (place_id) DO UPDATE SET
    name = EXCLUDED.name,
    address = EXCLUDED.address,
    city = EXCLUDED.city,
    category = EXCLUDED.category,
    rating = EXCLUDED.rating,
    user_ratings_total = EXCLUDED.user_ratings_total,
    hours_weekly = EXCLUDED.hours_weekly,
    has_phone = EXCLUDED.has_phone,
    has_website = EXCLUDED.has_website,
    location = EXCLUDED.location,
    istat_code = EXCLUDED.istat_code;
