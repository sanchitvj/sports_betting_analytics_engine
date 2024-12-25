-- Snowpipe not required in this case as we are using DAG to refresh

CREATE OR REPLACE PIPE sports_db.raw_layer.nba_games_pipe
AUTO_INGEST = true
AS
COPY INTO sports_db.raw_layer.nba_games
FROM @sports_db.raw_layer.nba_games_stage
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


CREATE OR REPLACE PIPE sports_db.raw_layer.nba_odds_pipe
AUTO_INGEST = true
AS
COPY INTO sports_db.raw_layer.nba_odds
FROM @sports_db.raw_layer.nba_odds_stage
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


CREATE OR REPLACE PIPE sports_db.raw_layer.nhl_games_pipe
AUTO_INGEST = true
AS
COPY INTO sports_db.raw_layer.nhl_games
FROM @sports_db.raw_layer.nhl_games_stage
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


CREATE OR REPLACE PIPE sports_db.raw_layer.nhl_oddss_pipe
AUTO_INGEST = true
AS
COPY INTO sports_db.raw_layer.nhl_oddss
FROM @sports_db.raw_layer.nhl_oddss_stage
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


CREATE OR REPLACE PIPE sports_db.raw_layer.nfl_games_pipe
AUTO_INGEST = true
AS
COPY INTO sports_db.raw_layer.nfl_games
FROM @sports_db.raw_layer.nfl_games_stage
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


CREATE OR REPLACE PIPE sports_db.raw_layer.nfl_odds_pipe
AUTO_INGEST = true
AS
COPY INTO sports_db.raw_layer.nfl_odds
FROM @sports_db.raw_layer.nfl_odds_stage
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


CREATE OR REPLACE PIPE sports_db.raw_layer.cfb_games_pipe
AUTO_INGEST = true
AS
COPY INTO sports_db.raw_layer.cfb_games
FROM @sports_db.raw_layer.cfb_games_stage
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


CREATE OR REPLACE PIPE sports_db.raw_layer.cfb_odds_pipe
AUTO_INGEST = true
AS
COPY INTO sports_db.raw_layer.cfb_odds
FROM @sports_db.raw_layer.cfb_odds_stage
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
