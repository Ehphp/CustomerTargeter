-- ============================================
-- build_places_raw.sql (schema allineato)
-- ============================================

BEGIN;
-- Pulisci la STAGING corretta (places_raw), non le sorgenti OSM
-- Usa CASCADE per rispettare i vincoli FK da places_clean (e derivati)
TRUNCATE TABLE places_raw RESTART IDENTITY CASCADE;
COMMIT;


-- Ricostruisci dai dati OSM
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
    location
)
SELECT
    'OSM_' || ob.osm_id                                           AS place_id,
    NULLIF(ob.name, '')                                           AS name,
    NULL::text                                                    AS formatted_address,
    NULLIF(ob.phone, '')                                          AS phone,
    NULLIF(ob.website, '')                                        AS website,
    CASE
      WHEN ob.category IS NOT NULL OR ob.subtype IS NOT NULL
      THEN ARRAY_REMOVE(ARRAY[
             NULLIF(ob.category, ''),
             NULLIF(ob.subtype,  '')
           ], NULL)::text[]
      ELSE NULL
    END                                                           AS types,
    NULL::numeric                                                 AS rating,
    NULL::integer                                                 AS user_ratings_total,
    CASE
      WHEN NULLIF(ob.opening_hours,'') IS NOT NULL
      THEN jsonb_build_object('opening_hours', ob.opening_hours)::jsonb
      ELSE NULL
    END                                                           AS opening_hours_json,
    ob.location                                                   AS location
FROM osm_business ob
WHERE ob.category IS NOT NULL;   -- ⛔ esclude category NULL

COMMIT;
