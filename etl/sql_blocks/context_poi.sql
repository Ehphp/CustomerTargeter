WITH poi AS (SELECT location::geometry AS g FROM osm_poi),
nearest AS (
  SELECT p.place_id, AVG(ST_Distance(p.location::geometry, o.g)) AS dist_avg
  FROM places_clean p JOIN poi o ON TRUE
  GROUP BY p.place_id
)
INSERT INTO place_context(place_id, distance_poi_avg)
SELECT place_id, GREATEST(0, dist_avg) FROM nearest
ON CONFLICT(place_id) DO UPDATE SET distance_poi_avg=EXCLUDED.distance_poi_avg;
