UPDATE company_scores
SET total_score = COALESCE(popularity_score, 0)
                + COALESCE(territory_score, 0)
                + COALESCE(accessibility_score, 0);
