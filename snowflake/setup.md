### Snowflake Policy

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:DeleteObject",
                "s3:DeleteObjectVersion"
            ],
            "Resource": [
                "arn:aws:s3:::raw-sp-data-aeb/*",
                "arn:aws:s3:::cur-sp-data-aeb/*",
                "arn:aws:s3:::hist-sp-data-aeb/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::raw-sp-data-aeb",
                "arn:aws:s3:::cur-sp-data-aeb",
                "arn:aws:s3:::hist-sp-data-aeb"
            ]
        }
    ]
}
```

Use this to check if connections is successful.
`LIST @SPORTS_DB.RAW_LAYER.GAMES_STAGE;`


## Steps to reset

1. Recreate the external stages using this [script](external_stages.sql).
2. Recreate the external tables using this [script](external_tables.sql).
3. Check database and query data to cross-check if there are any issues.

> Possible error  
> *Message: External table NHL_ODDS marked invalid. Missing or invalid file format 'PARQUET_FORMAT'*
