UPDATE company_scores s
SET territory_score = ROUND(
  (
    (1 - LEAST(pc.density_500m / 50.0, 1)) * 0.4 +         -- aree meno dense = punteggio più alto
    GREATEST(0, LEAST(1, 1 - pc.distance_poi_avg / 500.0)) * 0.6  -- vicinanza ai POI = più centrale
  ) * 15.0,                                                -- porta su scala 0..15
  6
)
FROM place_context pc
WHERE s.place_id = pc.place_id;
