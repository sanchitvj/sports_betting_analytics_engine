# Sports Betting Analytics Engine

### Real-time pipeline
```mermaid
graph LR
    classDef ingestion fill:#e6f3ff,stroke:#3498db
    classDef processing fill:#e6ffe6,stroke:#2ecc71
    classDef storage fill:#fff0e6,stroke:#e67e22
    classDef analytics fill:#ffe6e6,stroke:#e74c3c
    classDef monitoring fill:#f0e6ff,stroke:#9b59b6

    %% Real-time Pipeline
    A[Real-time APIs] -->|Stream| B[Kafka]
    B -->|Process| C[Spark Streaming]
    B -->|Backup| D[raw-s3/topics]
    C -->|Real-time Analytics| E[Apache Druid]
    E -->|Live Dashboard| F[Grafana RT]
    E -->|Archive| G[hist-s3/druid/segments]

    A & B:::ingestion
    C:::processing
    D & G:::storage
    E:::analytics
    F:::monitoring
```

### Batch pipeline
```mermaid
graph LR
    classDef source fill:#e6f3ff,stroke:#3498db
    classDef processing fill:#e6ffe6,stroke:#2ecc71
    classDef storage fill:#fff0e6,stroke:#e67e22
    classDef analytics fill:#ffe6e6,stroke:#e74c3c
    classDef quality fill:#f5f5f5,stroke:#7f8c8d

    %% Batch Pipeline
    A[Historical APIs] -->|Ingest| B[Airflow DAGs]
    B -->|Raw Data| C[raw-s3/historical]
    C -->|Transform| D[AWS Glue]
    D -->|Process| E[Iceberg Tables]
    E -->|Load| F[Snowflake Gold]
    F -->|Model| G[dbt]
    G -->|Visualize| H[Grafana Batch]

    E -->|Catalog| I[Glue Catalog]
    E -->|Store| J[cur-s3/processed]

    A:::source
    B & D:::processing
    C & J:::storage
    F & G:::analytics
    H:::analytics
    I:::quality
```

### DAGs
```mermaid
graph TB
    classDef source fill:#e6f3ff,stroke:#3498db
    classDef validation fill:#ffe6e6,stroke:#e74c3c
    classDef storage fill:#fff0e6,stroke:#e67e22
    classDef processing fill:#e6ffe6,stroke:#2ecc71
    classDef task fill:#f0e6ff,stroke:#9b59b6

    subgraph "Ingestion DAG"
        A[Historical APIs]:::source --> B[Check Source Data]:::validation
        B --> C{Data Exists?}
        C -->|Yes| D[Fetch Data]:::task
        D --> E[Validate JSON]:::validation
        E --> F[Upload to S3]:::storage
        C -->|No| G[Skip Processing]:::task
    end
```

```mermaid
graph TB
    classDef source fill:#e6f3ff,stroke:#3498db
    classDef validation fill:#ffe6e6,stroke:#e74c3c
    classDef storage fill:#fff0e6,stroke:#e67e22
    classDef processing fill:#e6ffe6,stroke:#2ecc71
    classDef task fill:#f0e6ff,stroke:#9b59b6
    subgraph "Processing DAG"
        H[Raw S3 Data]:::source --> I[Check Data Availability]:::validation
        I --> J{Data Ready?}
        J -->|Yes| K[Upload Glue Script]:::task
        K --> L[Setup Glue Job]:::task
        L --> M[Process Data]:::processing
        M --> N[Quality Checks]:::validation
        N --> O[Iceberg Tables]:::storage
        J -->|No| P[Skip Day]:::task
    end
```

```mermaid
graph TB
    classDef source fill:#e6f3ff,stroke:#3498db
    classDef validation fill:#ffe6e6,stroke:#e74c3c
    classDef storage fill:#fff0e6,stroke:#e67e22
    classDef processing fill:#e6ffe6,stroke:#2ecc71
    classDef task fill:#f0e6ff,stroke:#9b59b6
    subgraph "Task Groups"
        Q[NBA Pipeline]:::task
        R[NFL Pipeline]:::task
        S[NHL Pipeline]:::task
        T[NCAAF Pipeline]:::task
        
        Q & R & S & T --> U[Parallel Processing]:::processing
    end
```

```mermaid
graph TB
    subgraph "Data Sources"
        A[Iceberg Tables] -->|Load| B[Snowflake External Tables]
    end
    
    subgraph "dbt Transformations"
        B -->|Transform| C[Base Models]
        C -->|Aggregate| D[Intermediate Models]
        D -->|Business Logic| E[Gold Tables]
    end
    
    subgraph "Analytics"
        E -->|Query| F[BI Dashboards]
        E -->|Analysis| G[ML Models]
    end
```
