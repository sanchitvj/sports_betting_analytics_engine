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
