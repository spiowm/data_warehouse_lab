# Data Warehouse Lab

Навчальний проєкт на базі **Apache Airflow** (Docker Compose) для ETL та побудови
сховища даних на основі датасету температур `GlobalLandTemperaturesByCity`.

## Структура

- `dags/temperature_etl_dag/` — ETL-пайплайн: читає CSV і формує датасети
  `dim_city`, `dim_date`, `fact_monthly_temperatures` (DAG `temperature_data_etl_strict_schema`).
- `dags/dwh_setup_and_load_dag/` — створення таблиць, завантаження та агрегація
  даних у DWH через SQL (DAG `lab3_dwh_load_and_aggregate_v2`).
- `config/airflow.cfg` — конфігурація Airflow.
- `diagrams/model.puml` — модель сховища (PlantUML).
- `docker-compose.yaml` — Airflow + PostgreSQL + Redis.

## Запуск

```bash
cp .env.example .env        # AIRFLOW_UID=50000
docker compose up -d
```

Airflow UI: http://localhost:8080 (логін/пароль за замовчуванням `airflow`/`airflow`).
