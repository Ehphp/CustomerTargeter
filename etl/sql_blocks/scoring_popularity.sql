WITH
-- 1) visibilità su strada (0..1)
road_near AS (
  SELECT p.place_id, r.highway,
         ST_Distance(p.location::geometry, r.geom::geometry) AS dist_m
  FROM places_clean p
  JOIN osm_roads r
    -- allineo raggio a 50 m (come l’attenuazione)
    ON ST_DWithin(p.location::geometry, r.geom::geometry, 50)
),
road_rank AS (
  SELECT place_id,
         -- PRENDO LA STRADA MIGLIORE, NON LA PEGGIORE
         MAX(
           CASE highway
             WHEN 'motorway'     THEN 1.00
             WHEN 'trunk'        THEN 0.95
             WHEN 'primary'      THEN 0.90
             WHEN 'secondary'    THEN 0.80
             WHEN 'tertiary'     THEN 0.70
             WHEN 'unclassified' THEN 0.55
             WHEN 'residential'  THEN 0.45
             WHEN 'service'      THEN 0.30
             WHEN 'footway'      THEN 0.15
             ELSE 0.25
           END * GREATEST(0, (1 - dist_m/50.0))
         ) AS road_score   -- 0..1
  FROM road_near
  GROUP BY place_id
),
-- 2) centralità (0..1)
centrality AS (
  SELECT place_id,
         GREATEST(0, LEAST(1, 1 - distance_poi_avg/500.0)) AS centrality_norm
  FROM place_context
)

-- 3) Popularità (0–50) = 60% strada + 40% centralità
INSERT INTO company_scores(place_id, popularity_score)
SELECT p.place_id,
       ROUND( ((COALESCE(r.road_score, 0) * 0.6 + COALESCE(c.centrality_norm, 0) * 0.4) * 25.0)::numeric , 6 )
FROM places_clean p
LEFT JOIN road_rank  r ON r.place_id = p.place_id
LEFT JOIN centrality c ON c.place_id = p.place_id
ON CONFLICT(place_id) DO UPDATE
SET popularity_score = EXCLUDED.popularity_score;
