CREATE TABLE IF NOT EXISTS place_sector_density (
  place_id TEXT PRIMARY KEY REFERENCES places_clean(place_id) ON DELETE CASCADE,
  sector TEXT,
  neighbor_count INT,
  density_score NUMERIC,
  computed_at TIMESTAMP DEFAULT now()
);

WITH neighbors AS (
  SELECT
    p.place_id,
    p.category AS sector,
    COUNT(*) FILTER (
      WHERE q.place_id IS NOT NULL
    ) AS neighbor_count
  FROM places_clean p
  LEFT JOIN places_clean q
    ON q.category = p.category
   AND q.place_id <> p.place_id
   AND q.location IS NOT NULL
   AND ST_DWithin(q.location, p.location, 500)
  WHERE p.category IS NOT NULL
  GROUP BY p.place_id, p.category
)
INSERT INTO place_sector_density (place_id, sector, neighbor_count, density_score, computed_at)
SELECT
  n.place_id,
  n.sector,
  n.neighbor_count,
  LEAST(n.neighbor_count / 30.0, 1.0),
  now()
FROM neighbors n
ON CONFLICT(place_id) DO UPDATE
SET
  sector = EXCLUDED.sector,
  neighbor_count = EXCLUDED.neighbor_count,
  density_score = EXCLUDED.density_score,
  computed_at = EXCLUDED.computed_at;
