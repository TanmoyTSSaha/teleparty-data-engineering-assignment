-- Q1: Top 20 movies by rating (min 10k votes)
SELECT
    primaryTitle,
    averageRating,
    numVotes,
    startYear
FROM titles_enriched
WHERE titleType = 'movie'
  AND numVotes >= 10000
ORDER BY averageRating DESC, numVotes DESC
LIMIT 20;

-- Q2: Episodes per season for top 10 series by votes
WITH top_series AS (
    SELECT parentTconst
    FROM episodes_enriched
    GROUP BY parentTconst
    ORDER BY MAX(seriesNumVotes) DESC
    LIMIT 10
)
SELECT
    e.seriesTitle,
    e.seasonNumber,
    COUNT(*) AS episode_count
FROM episodes_enriched e
INNER JOIN top_series t ON e.parentTconst = t.parentTconst
GROUP BY e.seriesTitle, e.seasonNumber
ORDER BY e.seriesTitle, e.seasonNumber;

-- Q3: Avg rating by primary genre and decade
SELECT
    primary_genre,
    start_decade,
    AVG(averageRating) AS avg_rating,
    COUNT(*) AS title_count
FROM titles_enriched
WHERE primary_genre IS NOT NULL
  AND averageRating IS NOT NULL
GROUP BY primary_genre, start_decade
ORDER BY start_decade, avg_rating DESC;

-- Q4: Episode vs parent series average rating gap
SELECT
    seriesTitle,
    AVG(episodeAverageRating) AS avg_episode_rating,
    AVG(seriesAverageRating) AS avg_series_rating,
    AVG(episodeAverageRating) - AVG(seriesAverageRating) AS rating_gap
FROM episodes_enriched
WHERE episodeAverageRating IS NOT NULL
  AND seriesAverageRating IS NOT NULL
GROUP BY seriesTitle
HAVING COUNT(*) >= 10
ORDER BY rating_gap DESC
LIMIT 20;

-- Q5: Top 25 actors by credit count
SELECT
    primaryName,
    COUNT(*) AS credit_count
FROM cast_credits
WHERE category IN ('actor', 'actress')
GROUP BY primaryName
ORDER BY credit_count DESC
LIMIT 25;

-- Q6: Avg runtime by title type and decade
SELECT
    titleType,
    start_decade,
    AVG(runtimeMinutes) AS avg_runtime_minutes,
    COUNT(*) AS title_count
FROM titles_enriched
WHERE runtimeMinutes IS NOT NULL
GROUP BY titleType, start_decade
ORDER BY start_decade, titleType;
