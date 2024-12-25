CREATE OR REPLACE STAGE SPORTS_DB.RAW_LAYER.NBA_GAMES_STAGE
  STORAGE_INTEGRATION = s3_sports_integration
  URL = 's3://cur-sp-data-aeb/processed/aeb_glue_db.db/nba_games'
  FILE_FORMAT = SPORTS_DB.RAW_LAYER.PARQUET_FORMAT;


CREATE OR REPLACE STAGE SPORTS_DB.RAW_LAYER.NFL_GAMES_STAGE
  STORAGE_INTEGRATION = s3_sports_integration
  URL = 's3://cur-sp-data-aeb/processed/aeb_glue_db.db/nfl_games'
  FILE_FORMAT = SPORTS_DB.RAW_LAYER.PARQUET_FORMAT;


CREATE OR REPLACE STAGE SPORTS_DB.RAW_LAYER.CFB_GAMES_STAGE
  STORAGE_INTEGRATION = s3_sports_integration
  URL = 's3://cur-sp-data-aeb/processed/aeb_glue_db.db/cfb_games'
  FILE_FORMAT = SPORTS_DB.RAW_LAYER.PARQUET_FORMAT;


CREATE OR REPLACE STAGE SPORTS_DB.RAW_LAYER.NHL_GAMES_STAGE
  STORAGE_INTEGRATION = s3_sports_integration
  URL = 's3://cur-sp-data-aeb/processed/aeb_glue_db.db/nhl_games'
  FILE_FORMAT = SPORTS_DB.RAW_LAYER.PARQUET_FORMAT;


CREATE OR REPLACE STAGE SPORTS_DB.RAW_LAYER.NBA_ODDS_STAGE
  STORAGE_INTEGRATION = s3_sports_integration
  URL = 's3://cur-sp-data-aeb/processed/aeb_glue_db.db/nba_odds'
  FILE_FORMAT = SPORTS_DB.RAW_LAYER.PARQUET_FORMAT;


CREATE OR REPLACE STAGE SPORTS_DB.RAW_LAYER.NFL_ODDS_STAGE
  STORAGE_INTEGRATION = s3_sports_integration
  URL = 's3://cur-sp-data-aeb/processed/aeb_glue_db.db/nfl_odds'
  FILE_FORMAT = SPORTS_DB.RAW_LAYER.PARQUET_FORMAT;


CREATE OR REPLACE STAGE SPORTS_DB.RAW_LAYER.CFB_ODDS_STAGE
  STORAGE_INTEGRATION = s3_sports_integration
  URL = 's3://cur-sp-data-aeb/processed/aeb_glue_db.db/cfb_odds'
  FILE_FORMAT = SPORTS_DB.RAW_LAYER.PARQUET_FORMAT;


CREATE OR REPLACE STAGE SPORTS_DB.RAW_LAYER.NHL_ODDS_STAGE
  STORAGE_INTEGRATION = s3_sports_integration
  URL = 's3://cur-sp-data-aeb/processed/aeb_glue_db.db/nhl_odds'
  FILE_FORMAT = SPORTS_DB.RAW_LAYER.PARQUET_FORMAT;
