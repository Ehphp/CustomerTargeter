BEGIN;

-- Svuota tabelle in ordine di dipendenze per evitare violazioni FK
TRUNCATE TABLE enrichment_response RESTART IDENTITY;
TRUNCATE TABLE enrichment_request RESTART IDENTITY;
TRUNCATE TABLE business_metrics RESTART IDENTITY;
TRUNCATE TABLE business_facts RESTART IDENTITY;
TRUNCATE TABLE place_sector_density RESTART IDENTITY;
TRUNCATE TABLE places_clean RESTART IDENTITY;
TRUNCATE TABLE places_raw RESTART IDENTITY;
-- opzionale: reset dei POI derivati
-- TRUNCATE TABLE osm_poi RESTART IDENTITY;

COMMIT;
