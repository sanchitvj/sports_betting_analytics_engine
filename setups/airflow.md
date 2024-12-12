
We need to modify `airflow.cfg` in order to handle 4 DAGs each with 4 tasks working on backfilling for past 2 years of data.

```
max_threads = 8
scheduler_heartbeat_sec = 1
parsing_processes = 8
dag_file_processor_timeout = 600
```