# Betflow

## Contents of Repository
1. [Direction](#1-directions-to-use)
2. [Infra Setup](#2-infrastructure-setup)
3. [Batch Pipelines](#3-batch-pipelines)
4. [Demo](#4-demo-screenshots)
5. [Running pipelines](#5-running-real-time-pipelines)
6. [Dashboard](#6-dashboard)


### 1. Directions to use

1. Clone repository
`git clone https://github.com/sanchitvj/sports_betting_analytics_engine.git`
2. Change directory
`cd sports_betting_analytics_engine`
3. Install package betflow using
`pip install .`  
If you want to install in editable mode `pip install -e .`
4. Make an environment file named `my.env` and add keys for the APIs you want to try    
```bash
ODDS_API_KEY=
OPEN_WEATHER_API_KEY=
GLUE_ROLE=
GITHUB_TOKEN=
GNEWS_API_KEY=
```

### 2. Infrastructure Setup

Check below links for there [setups](setups/README.md)  
[Apache Airflow](setups/airflow.md)  
[Apache Druid](setups/druid.md)  
[Apache Kafka and S3-Connector](setups/kafka.md)  
[dbt](setups/dbt.md)  
[Grafana](setups/grafana.md)  
[Snowflake](setups/snowflake.md)  

### 3. Batch Pipelines
Detailed explanation of pipelines and lineage:  
[Airflow](dags/README.md)  
[dbt](dbt/README.md)

### 4. Demo screenshots

[Demo](demo.md)

### 5. Running real-time pipelines
- `cd src/pipeline_mains`
- Games: `python games_pipeline`
- Odds: `python odds_pipeline`
- Weather: `python weather_pipeline`
  
### 6. Dashboard
Some of the queries used for visualization in Grafana are [here](src/grafana_viz_queries).