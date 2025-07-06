# TFT Lakehouse with Riot API

## Stack:
- Airflow for orchestration
- Riot API ingestion DAG
- Iceberg-ready architecture (MinIO as S3)
- Docker Compose based

## Usage:
1. Set your Riot API key in `airflow.env`
2. Run `docker-compose up`
3. Airflow Web UI on localhost:8080
