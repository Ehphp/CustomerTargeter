CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE places_raw (
  place_id TEXT PRIMARY KEY,
  name TEXT,
  formatted_address TEXT,
  phone TEXT,
  website TEXT,
  types TEXT[],
  rating NUMERIC,
  user_ratings_total INT,
  opening_hours_json JSONB,
  location GEOGRAPHY(POINT, 4326),
  source_ts TIMESTAMP DEFAULT now()
);

CREATE TABLE osm_poi (
  osm_id TEXT PRIMARY KEY,
  poi_type TEXT,
  name TEXT,
  tags JSONB,
  location GEOGRAPHY(POINT, 4326)
);

CREATE TABLE istat_comuni (
  istat_code TEXT PRIMARY KEY,
  comune TEXT,
  provincia TEXT,
  regione TEXT,
  popolazione INT,
  superficie_km2 NUMERIC,
  presenze_turistiche INT,
  geom GEOMETRY(MULTIPOLYGON, 4326)
);

CREATE TABLE places_clean (
  place_id TEXT PRIMARY KEY REFERENCES places_raw(place_id),
  name TEXT,
  address TEXT,
  city TEXT,
  istat_code TEXT REFERENCES istat_comuni(istat_code),
  category TEXT,
  rating NUMERIC,
  user_ratings_total INT,
  hours_weekly INT,
  has_phone BOOLEAN,
  has_website BOOLEAN,
  location GEOGRAPHY(POINT, 4326)
);

CREATE TABLE IF NOT EXISTS brello_stations (
  station_id SERIAL PRIMARY KEY,
  name TEXT,
  geom GEOGRAPHY(POINT,4326),
  metadata JSONB,
  created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS geo_zones (
  zone_id SERIAL PRIMARY KEY,
  label TEXT NOT NULL,
  kind TEXT NOT NULL,
  priority INT NOT NULL DEFAULT 100,
  geom GEOMETRY(MULTIPOLYGON,4326) NOT NULL,
  created_at TIMESTAMP DEFAULT now()
);
-- Attività OSM (business)
CREATE TABLE IF NOT EXISTS osm_business (
  osm_id TEXT PRIMARY KEY,
  name TEXT,
  category TEXT,               -- da tag: shop / amenity / craft ...
  subtype TEXT,                -- es.: restaurant, bar, supermarket...
  tags JSONB,
  phone TEXT,
  website TEXT,
  opening_hours TEXT,
  location GEOGRAPHY(POINT,4326)
);

-- Strade OSM (minimo indispensabile per 'visibilità su strada')
CREATE TABLE IF NOT EXISTS osm_roads (
  osm_id TEXT PRIMARY KEY,
  highway TEXT,                -- primary, secondary, residential...
  name TEXT,
  geom GEOGRAPHY(LINESTRING,4326)
);

CREATE TABLE IF NOT EXISTS enrichment_request (
  request_id TEXT PRIMARY KEY,
  business_id TEXT REFERENCES places_clean(place_id) ON DELETE CASCADE,
  provider TEXT NOT NULL,
  input_hash TEXT NOT NULL,
  input_payload JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',
  error TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT now(),
  started_at TIMESTAMP,
  finished_at TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS enrichment_request_business_hash_idx
  ON enrichment_request (business_id, input_hash);

CREATE TABLE IF NOT EXISTS enrichment_response (
  response_id TEXT PRIMARY KEY,
  request_id TEXT REFERENCES enrichment_request(request_id) ON DELETE CASCADE,
  model TEXT,
  raw_response JSONB,
  parsed_response JSONB,
  prompt_tokens INT,
  completion_tokens INT,
  cost_cents NUMERIC,
  created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS business_facts (
  business_id TEXT PRIMARY KEY REFERENCES places_clean(place_id) ON DELETE CASCADE,
  size_class TEXT CHECK (size_class IN ('micro','piccola','media','grande')),
  is_chain BOOLEAN,
  website_url TEXT,
  social JSONB,
  marketing_attitude NUMERIC,
  umbrella_affinity NUMERIC,
  ad_budget_band TEXT CHECK (ad_budget_band IN ('basso','medio','alto')),
  budget_source TEXT,
  confidence NUMERIC,
  provenance JSONB,
  updated_at TIMESTAMP DEFAULT now(),
  source_provider TEXT,
  source_model TEXT
);

CREATE TABLE IF NOT EXISTS place_sector_density (
  place_id TEXT PRIMARY KEY REFERENCES places_clean(place_id) ON DELETE CASCADE,
  sector TEXT,
  neighbor_count INT,
  density_score NUMERIC,
  computed_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS business_metrics (
  business_id TEXT PRIMARY KEY REFERENCES places_clean(place_id) ON DELETE CASCADE,
  sector_density_neighbors INT,
  sector_density_score NUMERIC,
  geo_distribution_label TEXT,
  geo_distribution_source TEXT,
  size_class TEXT,
  is_chain BOOLEAN,
  ad_budget_band TEXT,
  umbrella_affinity NUMERIC,
  digital_presence NUMERIC,
  digital_presence_confidence NUMERIC,
  marketing_attitude NUMERIC,
  facts_confidence NUMERIC,
  updated_at TIMESTAMP DEFAULT now()
);
