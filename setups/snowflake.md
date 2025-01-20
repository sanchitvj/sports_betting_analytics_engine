### Snowflake Policy

[S3 Snowflake Storage Integration](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration)

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
                "arn:aws:s3:::raw-<>/*",
                "arn:aws:s3:::cur-<>/*",
                "arn:aws:s3:::hist-<>/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::raw-<>",
                "arn:aws:s3:::cur-<>",
                "arn:aws:s3:::hist-<>"
            ]
        }
    ]
}
```

Use this to check if connections is successful.
`LIST @SPORTS_DB.RAW_LAYER.GAMES_STAGE;`

### Snowflake MFA (where A stands for annoying)  
To disable MFA:  
`ALTER USER sanchitvj set DISABLE_MFA=true`

snowflake MFA + dbt ([doc](https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup#user--password--duo-mfa-authentication))  
add `authenticator: username_password_mfa` to your dbt `profiles.yml` and in your snowflake warehouse run:  
`alter account set allow_client_mfa_caching = true`