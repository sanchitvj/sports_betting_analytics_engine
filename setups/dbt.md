## Setting up dbt-core in EC2

1. Structure the `dbt` directory in project's root.
```
# give permissions
sudo chmod -R 755 ~/sports_betting_analytics_engine/dbt
```

2. Installations (I am using conda env).
```
pip install dbt-core==1.9.0b3 dbt-snowflake
```

3. Setting up profile
```
mkdir ~/.dbt
nano ~/.dbt/profiles.yml
```

```
# Profile should look like this, my project name is 'betflow'

betflow:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: account.region

      user: snowflake_user
      password: snowflake_password
      authenticator: username_password_mfa

      role: snowflake_role
      database: database_name
      warehouse: warehouse_name
      schema: staging
      threads: 4
      client_session_keep_alive: False
      query_tag: dbt

      connect_retries: 0
      connect_timeout: 1
      retry_on_database_errors: False
      retry_all: False
      reuse_connections: True
```

4. Check connections in project root
```
cd ~/sports_betting_analytics_engine
```
```
nano packages.yml

packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
  - package: calogica/dbt_expectations
    version: 0.10.3
  - package: dbt-labs/codegen
    version: 0.12.1
```
```
nano dbt_project.yml

name: 'betflow'
version: '1.0.0'
config-version: 2

profile: 'betflow'

model-paths: ["dbt/models"]
test-paths: ["dbt/tests"]
macro-paths: ["dbt/macros"]

target-path: "target"
clean-targets:
    - "target"
    - "dbt_modules"
    - "logs"

require-dbt-version: [">=1.0.0", "<2.0.0"]

models:
  betflow:
    staging:
      games:
        +materialized: view
        +schema: staging
      odds:
        +materialized: view
        +schema: staging
    marts:
      +materialized: table
      +schema: analytics
```

5. Check package installations
`dbt deps`

6. Run `dbt debug` to test connection with Snowflake. If you get `All checks passed!` at end then you are good to go.

For permission denied issues when doing `dbt deps`:
`sudo chown -R $USER:$USER <project_root>`