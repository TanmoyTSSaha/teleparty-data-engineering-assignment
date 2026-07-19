USE imdb;

CREATE TABLE IF NOT EXISTS cast_credits (
    tconst VARCHAR(20) NOT NULL,
    nconst VARCHAR(20) NOT NULL,
    category VARCHAR(50) NOT NULL,
    characters VARCHAR(1000),
    primaryName VARCHAR(300),
    titleType VARCHAR(32),
    primaryTitle VARCHAR(500),
    startYear INT,
    start_decade VARCHAR(16),
    primary_genre VARCHAR(50),
    averageRating DOUBLE,
    numVotes BIGINT
)
ENGINE=OLAP
DUPLICATE KEY(tconst, nconst, category)
DISTRIBUTED BY HASH(tconst) BUCKETS 16
PROPERTIES ("replication_num" = "1");
