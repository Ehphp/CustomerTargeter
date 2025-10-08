BEGIN;

-- Svuota tabelle in ordine di dipendenze per evitare violazioni FK
TRUNCATE TABLE company_scores RESTART IDENTITY;
TRUNCATE TABLE place_context RESTART IDENTITY;
TRUNCATE TABLE places_clean RESTART IDENTITY;
TRUNCATE TABLE places_raw RESTART IDENTITY;
-- opzionale: reset dei POI derivati
-- TRUNCATE TABLE osm_poi RESTART IDENTITY;

COMMIT;
