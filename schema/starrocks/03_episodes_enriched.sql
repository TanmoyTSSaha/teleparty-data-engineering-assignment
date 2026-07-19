USE imdb;

CREATE TABLE IF NOT EXISTS episodes_enriched (
    tconst VARCHAR(20) NOT NULL,
    parentTconst VARCHAR(20),
    seasonNumber INT,
    episodeNumber INT,
    episodeTitle VARCHAR(500),
    seriesTitle VARCHAR(500),
    episodeStartYear INT,
    seriesStartYear INT,
    episodeRuntimeMinutes INT,
    seriesRuntimeMinutes INT,
    episodeGenres VARCHAR(200),
    seriesGenres VARCHAR(200),
    episodePrimaryGenre VARCHAR(50),
    seriesPrimaryGenre VARCHAR(50),
    episodeAverageRating DOUBLE,
    episodeNumVotes BIGINT,
    seriesAverageRating DOUBLE,
    seriesNumVotes BIGINT,
    start_decade VARCHAR(16)
)
ENGINE=OLAP
DUPLICATE KEY(tconst)
DISTRIBUTED BY HASH(parentTconst) BUCKETS 8
PROPERTIES ("replication_num" = "1");
