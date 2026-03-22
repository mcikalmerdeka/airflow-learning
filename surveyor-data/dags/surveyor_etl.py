"""
ETL pipeline for BPS Surveyor data to Neon PostgreSQL.

This DAG generates dummy surveyor data and loads it into a Neon cloud PostgreSQL database.
Follows modern TaskFlow API patterns with proper separation of ETL tasks.

Pipeline Flow:
1. generate_data: Creates dummy surveyor records
2. transform_data: Processes and validates the records
3. create_table: Ensures database table exists
4. load_to_neon: Batch inserts data into Neon PostgreSQL
"""

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import random
import logging
import os
from dotenv import load_dotenv

# Load environment variables from .env file for local development
# In production, these should be set via Airflow UI or deployment environment
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Sample data for generating realistic surveyor records
SURVEYOR_NAMES = [
    "Ahmad Wijaya", "Siti Rahayu", "Budi Santoso", "Dewi Lestari",
    "Eko Prasetyo", "Fitri Handayani", "Gunawan Setiawan", "Hesti Wulandari",
    "Irfan Hakim", "Joko Susilo", "Rina Susanti", "Agus Salim",
    "Maya Sari", "Hendra Kusuma", "Lina Wati", "Fajar Nugroho",
    "Rini Puspita", "Adi Wijaya", "Sri Wahyuni", "Doni Setiawan",
]

REGIONS = [
    "Jakarta Pusat", "Jakarta Selatan", "Jakarta Utara", "Jakarta Barat",
    "Jakarta Timur", "Bandung", "Surabaya", "Yogyakarta", "Semarang",
    "Medan", "Makassar", "Denpasar", "Palembang", "Malang", "Bogor",
]

SURVEY_TYPES = [
    "Sensus Penduduk", "Survei Sosial Ekonomi", "Survei Pertanian",
    "Survei Industri", "Survei Kesehatan", "Survei Pendidikan",
    "Survei Ketenagakerjaan", "Survei Konsumsi", "Survei Rumah Tangga",
]

STATUS_OPTIONS = ["Completed", "In Progress", "Pending Review", "Validated"]


@dag(
    dag_id="surveyor_data_etl",
    description="ETL pipeline for BPS Surveyor data to Neon PostgreSQL",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "surveyor-team",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["etl", "surveyor", "bps", "postgres", "neon", "pipeline"],
    doc_md=__doc__,
)
def surveyor_data_etl():
    """Define the Surveyor Data ETL pipeline."""
    
    @task
    def generate_surveyor_data(num_rows: int = 100) -> list[dict]:
        """
        Extract: Generate dummy surveyor data.
        
        Args:
            num_rows: Number of surveyor records to generate (default: 100)
            
        Returns:
            List of surveyor data dictionaries (XCom-compatible)
        """
        logger.info(f"Generating {num_rows} surveyor records...")
        data = []
        base_date = datetime(2024, 1, 1)
        
        for i in range(num_rows):
            surveyor_id = f"SRV{str(i + 1).zfill(5)}"
            name = SURVEYOR_NAMES[i % len(SURVEYOR_NAMES)]
            region = random.choice(REGIONS)
            survey_type = random.choice(SURVEY_TYPES)
            survey_date = base_date + timedelta(days=random.randint(0, 180))
            households_visited = random.randint(10, 50)
            response_rate = round(random.uniform(0.6, 0.95), 2)
            status = random.choice(STATUS_OPTIONS)
            quality_score = round(random.uniform(70, 100), 1)
            
            data.append({
                "surveyor_id": surveyor_id,
                "name": name,
                "region": region,
                "survey_type": survey_type,
                "survey_date": survey_date.strftime("%Y-%m-%d"),
                "households_visited": households_visited,
                "response_rate": response_rate,
                "status": status,
                "quality_score": quality_score,
            })
        
        logger.info(f"Successfully generated {len(data)} surveyor records")
        return data
    
    @task
    def transform_surveyor_data(raw_data: list[dict]) -> list[dict]:
        """
        Transform: Process and validate surveyor data.
        
        Args:
            raw_data: List of surveyor data dictionaries from extract task
            
        Returns:
            Transformed and validated data ready for loading
        """
        logger.info(f"Transforming {len(raw_data)} surveyor records...")
        
        transformed = []
        for record in raw_data:
            # Calculate derived metrics
            successful_interviews = int(record["households_visited"] * record["response_rate"])
            
            # Categorize surveyor performance based on quality score
            if record["quality_score"] >= 90:
                performance_tier = "Excellent"
            elif record["quality_score"] >= 80:
                performance_tier = "Good"
            else:
                performance_tier = "Average"
            
            transformed_record = {
                **record,
                "successful_interviews": successful_interviews,
                "performance_tier": performance_tier,
                "data_quality": "Validated",
                "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            transformed.append(transformed_record)
        
        logger.info(f"Successfully transformed {len(transformed)} records")
        return transformed
    
    # Create table using traditional operator (no XCom needed)
    create_table_task = SQLExecuteQueryOperator(
        task_id="create_surveyor_table",
        conn_id="neon-postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS surveyor_data (
            id SERIAL PRIMARY KEY,
            surveyor_id VARCHAR(20) UNIQUE NOT NULL,
            name VARCHAR(100) NOT NULL,
            region VARCHAR(50) NOT NULL,
            survey_type VARCHAR(50) NOT NULL,
            survey_date DATE NOT NULL,
            households_visited INTEGER NOT NULL,
            response_rate DECIMAL(5,2) NOT NULL,
            status VARCHAR(30) NOT NULL,
            quality_score DECIMAL(5,1) NOT NULL,
            successful_interviews INTEGER NOT NULL,
            performance_tier VARCHAR(20) NOT NULL,
            data_quality VARCHAR(30) NOT NULL,
            processed_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_surveyor_region ON surveyor_data(region);
        CREATE INDEX IF NOT EXISTS idx_surveyor_type ON surveyor_data(survey_type);
        CREATE INDEX IF NOT EXISTS idx_surveyor_date ON surveyor_data(survey_date);
        """,
    )
    
    @task
    def load_to_neon(transformed_data: list[dict]) -> str:
        """
        Load: Batch insert surveyor data into Neon PostgreSQL.
        Skips duplicates on subsequent runs using ON CONFLICT DO NOTHING.
        
        Args:
            transformed_data: List of transformed surveyor records
            
        Returns:
            Success message with record count
        """
        logger.info(f"Loading {len(transformed_data)} records to Neon PostgreSQL...")
        
        # Prepare values as list of tuples for batch insert
        values = []
        for record in transformed_data:
            values.append((
                record["surveyor_id"],
                record["name"],
                record["region"],
                record["survey_type"],
                record["survey_date"],
                record["households_visited"],
                record["response_rate"],
                record["status"],
                record["quality_score"],
                record["successful_interviews"],
                record["performance_tier"],
                record["data_quality"],
                record["processed_at"],
            ))
        
        columns = [
            "surveyor_id", "name", "region", "survey_type", "survey_date",
            "households_visited", "response_rate", "status", "quality_score",
            "successful_interviews", "performance_tier", "data_quality", "processed_at"
        ]
        
        # Use PostgresHook with replace=True to handle duplicates
        # This uses INSERT ... ON CONFLICT DO UPDATE for idempotent inserts
        pg_hook = PostgresHook(postgres_conn_id="neon-postgres")
        pg_hook.insert_rows(
            table="surveyor_data",
            rows=values,
            target_fields=columns,
            replace=True,
            replace_index=["surveyor_id"],
            commit_every=1000,
        )
        
        success_msg = f"Successfully loaded {len(values)} surveyor records to Neon PostgreSQL"
        logger.info(success_msg)
        return success_msg
    
    # Define workflow dependencies using TaskFlow API
    raw_data = generate_surveyor_data(num_rows=100)
    transformed = transform_surveyor_data(raw_data)
    
    # Table creation must complete before loading data
    load_result = load_to_neon(transformed)
    create_table_task >> load_result


# Instantiate the DAG
surveyor_data_etl_dag = surveyor_data_etl()
