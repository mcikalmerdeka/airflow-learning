"""
## Bitcoin Price ETL Pipeline

This DAG extracts Bitcoin price data from a CSV file, transforms it by adding
a price range calculation and normalizing dates, and loads it into a PostgreSQL database.

### Data Source
- CSV file containing historical Bitcoin price data (Open, High, Low, Close, Volume)

### Transformations
- Parse dates from string to datetime format
- Calculate price range (High - Low) as a new metric

### Load
- Insert data into PostgreSQL stock_data table
- Uses batch insert for better performance

### Schedule
- Runs daily at midnight
- No catchup (only processes current day)
"""

from datetime import timedelta
from pendulum import datetime

import pandas as pd

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from dotenv import load_dotenv

# Load environment variables from .env file for local development
# In production, these should be set via Airflow UI or deployment environment
load_dotenv()


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
def bitcoin_price_etl():
    """
    Bitcoin Price ETL Pipeline using TaskFlow API.
    
    This DAG follows the modern Airflow TaskFlow pattern with:
    - Separate tasks for Extract, Transform, and Load
    - Native Python data passing via XCom
    - PostgresHook for database operations
    - Proper error handling and serialization
    """

    # Create table in PostgreSQL with the price_range column included
    # Using SQLExecuteQueryOperator to execute SQL directly
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS stock_data (
        id SERIAL PRIMARY KEY,
        date TIMESTAMP,
        open_price DECIMAL(20, 6),
        high_price DECIMAL(20, 6),
        low_price DECIMAL(20, 6),
        close_price DECIMAL(20, 6),
        volume BIGINT,
        price_range DECIMAL(20, 6)
    );
    """
    
    create_table_task = SQLExecuteQueryOperator(
        task_id="create_table_postgres",
        sql=create_table_sql,
        conn_id="postgres-local",
    )

    @task
    def extract_data() -> list[dict]:
        """
        Extract Bitcoin price data from CSV file.
        
        Reads the BTC-USD.csv file from the include directory and returns
        the data as a list of dictionaries for XCom serialization.
        
        Returns:
            list[dict]: List of dictionaries containing raw Bitcoin price data
        """
        # Read the BTC stock data from CSV file using Docker-compatible path
        file_path = "/usr/local/airflow/include/BTC-USD.csv"
        df = pd.read_csv(file_path)
        
        # Convert DataFrame to list of dicts for XCom serialization
        return df.to_dict(orient="records")

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
        # Convert back to DataFrame for easier manipulation
        df = pd.DataFrame(raw_data)
        
        # Convert date column to datetime format
        df["date"] = pd.to_datetime(df["Date"])
        
        # Calculate price range (High - Low)
        df["price_range"] = df["High"] - df["Low"]
        
        # Convert Timestamp to string for JSON serialization (XCom compatibility)
        df["date"] = df["date"].astype(str)
        
        return df.to_dict(orient="records")

    @task
    def load_to_postgres(transformed_data: list[dict]) -> str:
        """
        Load transformed data into PostgreSQL.
        
        Uses PostgresHook for efficient batch insertion of Bitcoin price data
        into the stock_data table.
        
        Args:
            transformed_data: List of dictionaries containing transformed data
            
        Returns:
            str: Status message with count of loaded records
        """
        # Convert back to DataFrame
        df = pd.DataFrame(transformed_data)
        
        # Prepare data for insertion
        # Map DataFrame columns to database table columns
        columns = [
            "date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "price_range",
        ]
        
        # Extract values as list of tuples for batch insert
        values = [
            (
                row["date"],
                row["Open"],
                row["High"],
                row["Low"],
                row["Close"],
                row["Volume"],
                row["price_range"],
            )
            for _, row in df.iterrows()
        ]
        
        # Use PostgresHook for efficient database operations
        pg_hook = PostgresHook(postgres_conn_id="postgres-local")
        
        # Insert rows in batches for better performance
        pg_hook.insert_rows(
            table="stock_data",
            rows=values,
            target_fields=columns,
            replace=False,
            commit_every=1000,
        )
        
        return f"Successfully loaded {len(values)} Bitcoin price records to PostgreSQL"

    # Define the task flow using TaskFlow API
    # Data flows automatically through XCom
    raw_data = extract_data()
    transformed = transform_data(raw_data)
    load_result = load_to_postgres(transformed)
    
    # Set dependencies using bit-shift operators
    # create_table must complete before loading
    create_table_task >> load_result


# Instantiate the DAG
bitcoin_price_etl_dag = bitcoin_price_etl()
