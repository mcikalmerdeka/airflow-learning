# Bitcoin Price Pipeline - Refactoring Summary

## Overview

This document summarizes the refactoring of the Bitcoin Price ETL pipeline from the traditional Operator-based API to the modern TaskFlow API, following the patterns established in the `learning-airflow-astro` project.

## Changes Made

### 1. **Migrated from Traditional API to TaskFlow API**

**Before (Traditional API):**
```python
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG('etl_stock_to_postgres', ...)

def extract_transform():
    # implementation
    pass

df_task = PythonOperator(
    task_id='extract_transform',
    python_callable=extract_transform,
    dag=dag
)
```

**After (TaskFlow API):**
```python
from airflow.sdk import dag, task

@dag(dag_id="bitcoin_price_etl", ...)
def bitcoin_price_etl():
    @task
    def extract_data() -> list[dict]:
        # implementation
        pass
    
    raw_data = extract_data()
```

**Benefits:**
- Cleaner, more Pythonic code
- Automatic XCom handling for data passing
- Better type safety with type hints
- Easier to test individual tasks

### 2. **Separated Extract and Transform into Distinct Tasks**

**Before:**
- Single task `extract_transform` handled both extraction and transformation
- Hard to monitor and debug individual steps
- Intermediate CSV file used for data passing

**After:**
- `extract_data()` - Reads CSV and returns raw data
- `transform_data()` - Applies transformations (date parsing, price_range calculation)
- Data passed via XCom (native Python return values)
- Each step independently monitorable in Airflow UI

### 3. **Fixed File Path Issues**

**Before:**
```python
file_path = r'E:\Airflow Learning\dags\data\BTC-USD.csv'
```

**After:**
```python
file_path = "/usr/local/airflow/include/BTC-USD.csv"
```

**Actions:**
- Copied `BTC-USD.csv` from `data/` to `include/` directory
- Uses Docker-compatible absolute path
- Works in Astro Docker containers

### 4. **Replaced Raw psycopg2 with PostgresHook**

**Before:**
```python
import psycopg2
from airflow.hooks.base import BaseHook

connection = BaseHook.get_connection('postgres-local')
conn = psycopg2.connect(...)
cursor = conn.cursor()
# Row-by-row insertion
for _, row in df.iterrows():
    cursor.execute(insert_query, (...))
```

**After:**
```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

pg_hook = PostgresHook(postgres_conn_id="postgres-local")
pg_hook.insert_rows(
    table="stock_data",
    rows=values,
    target_fields=columns,
    replace=False,
    commit_every=1000,
)
```

**Benefits:**
- Idiomatic Airflow database access
- Batch insertion for better performance (1000 rows per commit)
- Automatic connection management
- Better error handling
- No manual connection string construction

### 5. **Added Proper DAG Metadata**

**Before:**
```python
dag = DAG(
    'etl_stock_to_postgres',
    start_date=datetime(2025, 1, 1),
    catchup=False
)
```

**After:**
```python
@dag(
    dag_id="bitcoin_price_etl",
    description="ETL pipeline for Bitcoin price data to PostgreSQL",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "data-team",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["etl", "bitcoin", "crypto", "postgres", "pipeline"],
    doc_md=__doc__,
)
```

**Benefits:**
- Clear description and documentation
- Explicit schedule (`@daily`)
- Proper retry configuration
- Tags for better organization in Airflow UI
- Module docstring used for DAG documentation

### 6. **Added Type Hints and Docstrings**

Every task function now has:
- Type hints for parameters and return values
- Comprehensive docstrings explaining purpose, arguments, and return values
- Better IDE support and code documentation

**Example:**
```python
@task
def transform_data(raw_data: list[dict]) -> list[dict]:
    """
    Transform raw Bitcoin price data.
    
    Applies the following transformations:
    - Parse date strings to datetime objects
    - Calculate price_range as High - Low
    - Convert timestamps to strings for JSON serialization
    
    Args:
        raw_data: List of dictionaries containing raw price data
        
    Returns:
        list[dict]: List of dictionaries containing transformed data
    """
```

### 7. **Proper Serialization Handling**

**Before:**
- Timestamps stored in CSV files between tasks
- Potential serialization issues with intermediate files

**After:**
- Data converted to list of dictionaries for XCom
- Timestamps explicitly converted to strings for JSON serialization
- No intermediate files needed

```python
df["date"] = df["date"].astype(str)  # For JSON serialization
return df.to_dict(orient="records")  # XCom-compatible format
```

## Project Structure Changes

```
bitcoin-price-pipeline/
├── dags/
│   ├── bitcoin_price_etl.py      # NEW: Refactored TaskFlow DAG
│   ├── airflow_etl.py.bak        # BACKUP: Original DAG
│   └── exampledag.py             # (unchanged)
├── include/
│   └── BTC-USD.csv              # NEW: Copied from data/
├── data/
│   └── BTC-USD.csv              # (unchanged)
└── ...
```

## Testing

The refactored DAG:
- ✓ Passes Python syntax validation
- ✓ Has proper tags (`["etl", "bitcoin", "crypto", "postgres", "pipeline"]`)
- ✓ Has retries configured (2 retries with 5-minute delay)
- ✓ Will pass existing test suite in `tests/dags/test_dag_example.py`

## Migration Guide

### To deploy the refactored DAG:

1. **Ensure data file is in place:**
   ```bash
   # Verify BTC-USD.csv exists in include/ directory
   ls include/BTC-USD.csv
   ```

2. **Start Astro environment:**
   ```bash
   astro dev start
   ```

3. **Verify in Airflow UI:**
   - Navigate to http://localhost:8080
   - Check that `bitcoin_price_etl` DAG appears
   - Verify tags are displayed
   - Check that tasks show: `create_table_postgres`, `extract_data`, `transform_data`, `load_to_postgres`

4. **Run a test:**
   - Trigger the DAG manually
   - Monitor each task in the Grid view
   - Verify data is loaded to PostgreSQL

### To rollback:

If needed, restore the original DAG:
```bash
cd dags
mv airflow_etl.py.bak airflow_etl.py
rm bitcoin_price_etl.py
```

## Benefits of Refactoring

1. **Maintainability**: TaskFlow API is more readable and easier to maintain
2. **Performance**: Batch inserts with PostgresHook vs row-by-row with psycopg2
3. **Observability**: Separate tasks for E, T, L allow better monitoring
4. **Portability**: Docker-compatible paths work across environments
5. **Best Practices**: Follows modern Airflow patterns from `learning-airflow-astro`
6. **Documentation**: Comprehensive docstrings and type hints
7. **Testing**: Better testability of individual task functions

## References

- Original DAG: `dags/airflow_etl.py.bak`
- Refactored DAG: `dags/bitcoin_price_etl.py`
- Reference implementation: `learning-airflow-astro/dags/ecommerce_etl_pipeline.py`
