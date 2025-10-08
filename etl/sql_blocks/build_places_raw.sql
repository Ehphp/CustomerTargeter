-- ============================================
-- build_places_raw.sql (incremental update)
-- ============================================

-- Ricostruisci/aggiorna i record staging partendo da osm_business
INSERT INTO places_raw (
    place_id,
    name,
    formatted_address,
    phone,
    website,
    types,
    rating,
    user_ratings_total,
    opening_hours_json,
    location,
    source_ts
)
SELECT
    CONCAT('OSM_', ob.osm_id)                                    AS place_id,
    NULLIF(ob.name, '')                                          AS name,
    NULL::text                                                   AS formatted_address,
    NULLIF(ob.phone, '')                                         AS phone,
    NULLIF(ob.website, '')                                       AS website,
    CASE
      WHEN ob.category IS NOT NULL OR ob.subtype IS NOT NULL
      THEN ARRAY_REMOVE(
        ARRAY[
          NULLIF(ob.category, ''),
          NULLIF(ob.subtype, '')
        ],
        NULL
      )::text[]
      ELSE NULL
    END                                                          AS types,
    NULL::numeric                                                AS rating,
    NULL::integer                                                AS user_ratings_total,
    CASE
      WHEN NULLIF(ob.opening_hours, '') IS NOT NULL
      THEN jsonb_build_object('opening_hours', ob.opening_hours)::jsonb
      ELSE NULL
    END                                                          AS opening_hours_json,
    ob.location                                                  AS location,
    now()                                                        AS source_ts
FROM osm_business ob
WHERE ob.category IS NOT NULL
ON CONFLICT (place_id) DO UPDATE
SET
    name = EXCLUDED.name,
    formatted_address = EXCLUDED.formatted_address,
    phone = EXCLUDED.phone,
    website = EXCLUDED.website,
    types = EXCLUDED.types,
    rating = EXCLUDED.rating,
    user_ratings_total = EXCLUDED.user_ratings_total,
    opening_hours_json = EXCLUDED.opening_hours_json,
    location = EXCLUDED.location,
    source_ts = EXCLUDED.source_ts;
