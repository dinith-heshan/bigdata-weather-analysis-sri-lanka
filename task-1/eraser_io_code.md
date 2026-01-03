title Sri Lanka Weather Analytics Platform – System Architecture

// 1. Data Sources (Blue)
Data Sources [color: blue, icon: database] {
  Historical CSVs [icon: file-text]
  Weather Sensors [icon: cpu, label: "Sensors / IoT"]
  External APIs [icon: cloud, label: "External APIs"]
}

// 2. Ingestion Layer (Orange)
Ingestion [color: orange, icon: upload] {
  Batch ETL [icon: airflow, label: "Batch ETL (Airflow)"]
  Streaming ETL [icon: kafka, label: "Streaming ETL (Kafka)"]
}

// 3. Storage Layer (Green)
Storage [color: green, icon: server] {
  Raw Data Storage [icon: aws-s3, label: "Raw Storage (HDFS)"]
  Processed Data Storage [icon: database, label: "Processed Storage (Delta Lake)"]
}

// 4. Processing / Analytics Layer (Purple)
Processing Analytics [color: purple, icon: activity, label: "Processing / Analytics"] {
  Batch Processing [icon: spark, label: "Batch Processing (Spark)"]
  Streaming Processing [icon: spark, label: "Streaming Processing (Spark Streaming)"]
  Analytics ML [icon: python, label: "Analytics / ML (Python, TensorFlow)"]
}

// 5. Serving / Visualization Layer (Red)
Serving Visualization [color: red, icon: bar-chart, label: "Serving / Visualization"] {
  API Layer [icon: flask, label: "API (FastAPI)"]
  Dashboard BI [icon: tableau, label: "Dashboard / BI Tools (Tableau)"]
}

// 6. Orchestration (Purple)
Orchestration [color: purple, icon: airflow, label: "Job Orchestration"] {
  Job Orchestration [icon: airflow, label: "Airflow"]
}

// 7. Monitoring / Logging (Gray)
Monitoring Logging [color: gray, icon: activity, label: "Monitoring / Logging"] {
  Observability [icon: prometheus, label: "Prometheus,Grafana"]
}

// 8. Security / Governance (Gray)
Security Governance [color: gray, icon: shield, label: "Security / Governance"] {
  IAM Roles [icon: user-check]
  Encryption [icon: lock]
  Audit Logs [icon: file-text, label: "Audit Logs"]
}

// Extras
Feature Store [icon: database, label: "Feature Store (Feast)"]
Caching Layer [icon: redis, label: "Caching Layer (Redis)"]
Alerts [icon: bell]

// Connections

// Batch Flow
Historical CSVs > Batch ETL
Batch ETL > Raw Data Storage
Raw Data Storage > Batch Processing
Batch Processing > Processed Data Storage

// Streaming Flow
Weather Sensors > Streaming ETL
External APIs > Streaming ETL
Streaming ETL > Streaming Processing
Streaming Processing > Processed Data Storage

// ML / Predictive Flow
Processed Data Storage > Feature Store
Feature Store > Analytics ML
Analytics ML > API Layer: predictions
API Layer > Dashboard BI
Processed Data Storage > Caching Layer
API Layer > Caching Layer
Dashboard BI < Caching Layer

// Orchestration
Job Orchestration --> Batch ETL: schedule
Job Orchestration --> Batch Processing: trigger
Job Orchestration --> Streaming ETL: manage
Job Orchestration --> Analytics ML: trigger

// Monitoring / Logging
Observability --> Ingestion: monitor
Observability --> Storage: monitor
Observability --> Processing Analytics: monitor
Observability --> Serving Visualization: monitor
Observability > Alerts

// Security / Governance
IAM Roles --> API Layer: access control
IAM Roles --> Storage: access control [color: Black]
Encryption --> Raw Data Storage: at rest
Encryption --> Streaming ETL: in transit
Encryption --> API Layer: in transit
Audit Logs --> API Layer: compliance
Audit Logs --> Ingestion: compliance

// Visualization
Processed Data Storage > API Layer: Visualization
