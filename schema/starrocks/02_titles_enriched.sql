USE imdb;

CREATE TABLE IF NOT EXISTS titles_enriched (
    tconst VARCHAR(20) NOT NULL,
    primaryTitle VARCHAR(500),
    isAdult INT,
    startYear INT,
    endYear INT,
    runtimeMinutes INT,
    genres VARCHAR(200),
    genres_array ARRAY<VARCHAR(50)>,
    primary_genre VARCHAR(50),
    averageRating DOUBLE,
    numVotes BIGINT,
    is_rated BOOLEAN,
    titleType VARCHAR(32),
    start_decade VARCHAR(16)
)
ENGINE=OLAP
DUPLICATE KEY(tconst)
DISTRIBUTED BY HASH(tconst) BUCKETS 8
PROPERTIES ("replication_num" = "1");
