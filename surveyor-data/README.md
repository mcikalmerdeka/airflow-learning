# Surveyor Data ETL Pipeline

A modern Apache Airflow ETL pipeline for processing BPS (Badan Pusat Statistik) surveyor data using the TaskFlow API and Neon cloud PostgreSQL.

## Overview

This project follows modern Airflow best practices with:
- **TaskFlow API**: Native Python data passing between tasks
- **Astro Runtime**: Managed Airflow distribution with pre-installed providers
- **Neon PostgreSQL**: Cloud-hosted PostgreSQL database for data storage
- **Modular Design**: Clear separation of Extract, Transform, and Load operations

## Project Structure

```
surveyor-data/
├── dags/
│   └── surveyor_etl.py          # Main ETL DAG with TaskFlow API
├── tests/
│   └── dags/
│       └── test_surveyor_dag.py # DAG validation tests
├── include/                     # Additional files (data, configs, SQL)
├── plugins/                     # Custom Airflow plugins
├── .astro/
│   └── config.yaml             # Astro CLI configuration
├── airflow_settings.yaml       # Local development connections (auto-loaded)
├── requirements.txt            # Python dependencies
├── Dockerfile                  # Astro Runtime image configuration
├── packages.txt                # OS-level packages
└── README.md                   # This file
```

## Architecture

### Pipeline Flow

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Generate      │     │    Transform     │     │   Create Table  │     │  Load to Neon   │
│   Surveyor Data │────▶│   & Validate     │────▶│   (if needed)   │────▶│   PostgreSQL    │
│   (Extract)     │     │   (Transform)    │     │                 │     │   (Load)        │
└─────────────────┘     └──────────────────┘     └─────────────────┘     └─────────────────┘
       @task                    @task                   SQLExecuteQuery        @task
```

### Data Model

**Table: `surveyor_data`**

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| surveyor_id | VARCHAR(20) | Unique surveyor identifier (e.g., SRV00001) |
| name | VARCHAR(100) | Surveyor full name |
| region | VARCHAR(50) | Survey region (e.g., Jakarta Pusat) |
| survey_type | VARCHAR(50) | Type of survey (e.g., Sensus Penduduk) |
| survey_date | DATE | Date of survey |
| households_visited | INTEGER | Number of households visited |
| response_rate | DECIMAL(5,2) | Survey response rate (0.0 - 1.0) |
| status | VARCHAR(30) | Survey status (Completed/In Progress/etc.) |
| quality_score | DECIMAL(5,1) | Quality score (0-100) |
| successful_interviews | INTEGER | Calculated: households × response_rate |
| performance_tier | VARCHAR(20) | Derived: Excellent/Good/Average |
| data_quality | VARCHAR(30) | Validation status |
| processed_at | TIMESTAMP | Processing timestamp |
| created_at | TIMESTAMP | Record creation timestamp |

## DAG Configuration

### Connection: `neon-postgres`

The pipeline connects to Neon cloud PostgreSQL using these settings (configured in `airflow_settings.yaml`):

```yaml
conn_id: neon-postgres
conn_type: postgres
conn_host: ep-old-resonance-ant8oqn7-pooler.c-6.us-east-1.aws.neon.tech
conn_port: 5432
conn_schema: neondb
conn_login: neondb_owner
conn_password: npg_R1DqakF7YMrV
conn_extra:
  sslmode: require
  channel_binding: require
```

### DAG Parameters

- **DAG ID**: `surveyor_data_etl`
- **Schedule**: Daily (`@daily`)
- **Start Date**: 2025-01-01
- **Retries**: 2 (with 5-minute delay)
- **Tags**: `etl`, `surveyor`, `bps`, `postgres`, `neon`, `pipeline`

## Getting Started

### Prerequisites

- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) installed
- Docker Desktop running
- Access to Neon PostgreSQL (configured in `airflow_settings.yaml`)

### Local Development

1. **Start the Airflow environment:**
   ```bash
   astro dev start
   ```

2. **Access the Airflow UI:**
   Open http://localhost:8080/ (default credentials: `admin`/`admin`)

3. **Verify the connection:**
   - Go to Admin → Connections
   - Check that `neon-postgres` connection exists

4. **Trigger the DAG:**
   - Navigate to DAGs → surveyor_data_etl
   - Click the play button to trigger a run

### Testing

Run DAG validation tests:

```bash
astro dev pytest
```

Parse DAGs to check for errors:

```bash
astro dev parse
```

### Generate Data Manually

You can also run the data generation script directly:

```bash
cd surveyor-data
python surveyor_dummy_data.py
```

## Task Details

### 1. generate_surveyor_data

Generates dummy surveyor records with:
- 20 unique surveyor names
- 15 Indonesian regions
- 9 survey types
- Random dates within 6-month window
- Metrics: households visited, response rate, quality score, status

**Returns**: List of dictionaries (XCom-compatible)

### 2. transform_surveyor_data

Transforms raw data:
- Calculates `successful_interviews` (households × response_rate)
- Assigns `performance_tier` based on quality_score
- Adds validation status and processing timestamp

**Input**: Raw data from extract task
**Returns**: Transformed data ready for loading

### 3. create_surveyor_table

Uses `SQLExecuteQueryOperator` to:
- Create table if not exists
- Create indexes on region, survey_type, and survey_date
- Ensures idempotent table creation

### 4. load_to_neon

Batch loads data using `PostgresHook`:
- Efficient batch inserts with `insert_rows()`
- 1000-row commit batches
- Returns success message with record count

## Development Best Practices

### Following Patterns from Bitcoin Price Pipeline

This project implements the same patterns:

1. **TaskFlow API**: Use `@dag` and `@task` decorators
2. **Type Hints**: Define data contracts between tasks
3. **XCom Serialization**: Return Python objects (lists/dicts), convert DataFrames
4. **Database Access**: Use PostgresHook with `insert_rows()` for batch operations
5. **Documentation**: Module-level docstrings for DAG docs
6. **Docker-Compatible Paths**: Use `/usr/local/airflow/include/` for data files
7. **Validation**: Parametrized pytest tests for DAG standards

### Code Organization

```python
@dag(...)  # Configuration at the top
def my_dag():
    @task
    def extract() -> list[dict]:
        """Task docstring."""
        pass
    
    @task
    def transform(data: list[dict]) -> list[dict]:
        """Input/output types defined."""
        pass
    
    # Define dependencies
    raw = extract()
    transformed = transform(raw)

my_dag()  # Instantiate
```

## Deployment

### Local Testing

```bash
# Parse DAGs for errors
astro dev parse

# Run tests
astro dev pytest

# Start environment
astro dev start
```

### Production Deployment

For production deployment to Astro or other environments:

1. **Update connection**: Replace local connection with production Neon credentials
2. **Environment variables**: Move sensitive data to environment variables
3. **CI/CD**: Use `astro deploy` or equivalent for your platform

## Troubleshooting

### Connection Issues

If you see connection errors:

1. Verify the `neon-postgres` connection in Airflow UI
2. Check that SSL parameters are set correctly:
   - `sslmode`: require
   - `channel_binding`: require
3. Test connectivity from Airflow worker

### DAG Import Errors

```bash
# Check for parse errors
astro dev parse

# View detailed error logs
astro dev logs
```

### Database Errors

- Verify Neon database is active (check Neon dashboard)
- Confirm credentials in `airflow_settings.yaml`
- Check network/firewall settings

## Resources

- [Airflow TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/taskflow.html)
- [Astro Documentation](https://www.astronomer.io/docs/)
- [Neon PostgreSQL](https://neon.tech/docs)
- [PostgresHook Documentation](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/_api/airflow/providers/postgres/hooks/postgres/index.html)

## License

This project follows the same patterns as the reference `bitcoin-price-pipeline` project.
