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

CREATE TABLE place_context (
  place_id TEXT PRIMARY KEY REFERENCES places_clean(place_id),
  density_500m INT,
  distance_poi_avg NUMERIC
);

CREATE TABLE company_scores (
  place_id TEXT PRIMARY KEY REFERENCES places_clean(place_id),
  popularity_score NUMERIC,
  territory_score NUMERIC,
  accessibility_score NUMERIC,
  total_score NUMERIC
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
