WITH c AS (
  SELECT place_id,
         (CASE WHEN has_phone THEN 1 ELSE 0 END +
          CASE WHEN has_website THEN 1 ELSE 0 END) AS contacts
  FROM places_clean
)
UPDATE company_scores s
SET accessibility_score = ROUND(
  (
    (p.hours_weekly / 84.0) * 0.7 +     -- orari lunghi = più accessibile
    (c.contacts / 2.0) * 0.3            -- presenza contatti = più accessibile
  ) * 10.0,                             -- porta tutto su scala 0..10
  6
)
FROM places_clean p
JOIN c ON c.place_id = p.place_id
WHERE s.place_id = p.place_id;
