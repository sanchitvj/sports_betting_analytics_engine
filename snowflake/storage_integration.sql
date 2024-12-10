CREATE OR REPLACE STORAGE INTEGRATION s3_sports_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::***********:role/snowflake_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://cur-sp-data-aeb/processed/aeb_glue_db.db/');

-- Get AWS IAM user for trust policy
DESC STORAGE INTEGRATION s3_sports_integration;



CREATE OR REPLACE FILE FORMAT SPORTS_DB.RAW_LAYER.PARQUET_FORMAT
  TYPE = PARQUET
  COMPRESSION = AUTO;