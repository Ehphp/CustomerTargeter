WITH buf AS (
  SELECT p.place_id, ST_Buffer(p.location::geography, 500)::geometry AS area
  FROM places_clean p
)
INSERT INTO place_context(place_id, density_500m)
SELECT b.place_id, COUNT(*) FILTER (WHERE q.place_id <> b.place_id)
FROM buf b
JOIN places_clean q ON ST_Within(q.location::geometry, b.area)
GROUP BY b.place_id
ON CONFLICT(place_id) DO UPDATE SET density_500m=EXCLUDED.density_500m;
