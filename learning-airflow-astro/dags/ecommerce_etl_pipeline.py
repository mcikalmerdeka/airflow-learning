"""
E-commerce ETL Pipeline DAG

This DAG demonstrates a simple ETL (Extract, Transform, Load) process:
1. EXTRACT: Generates random e-commerce order data using pandas
2. TRANSFORM: Cleans and enriches the data (calculates total amount, categorizes orders)
3. LOAD: Stores the transformed data into the PostgreSQL database

The pipeline appends new data on each run, making it easy to accumulate a dataset over time.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from pendulum import datetime as pendulum_datetime
from dotenv import load_dotenv

# Load environment variables from .env file for local development
# In production, these should be set via Airflow UI or deployment environment
load_dotenv()

# Default arguments for the DAG
default_args = {
    "owner": "learner",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="ecommerce_etl_pipeline",
    description="A simple ETL pipeline that generates random e-commerce data and loads it to PostgreSQL",
    start_date=pendulum_datetime(2024, 1, 1),
    schedule="*/5 * * * *",  # Run every 5 minutes for demo purposes
    catchup=False,
    default_args=default_args,
    tags=["etl", "ecommerce", "learning"],
    doc_md=__doc__,
)
def ecommerce_etl():
    
    @task
    def extract_generate_data(**context):
        """
        Extract: Generate random e-commerce order data
        Returns a pandas DataFrame with simulated order data
        """
        print("🚀 Starting data extraction/generation...")
        
        # Set random seed based on execution date for reproducibility
        execution_date = context['ds']
        random.seed(int(execution_date.replace('-', '')))
        np.random.seed(int(execution_date.replace('-', '')))
        
        # Generate random data
        n_orders = random.randint(10, 50)  # Random number of orders per run
        
        products = [
            ("Laptop", "Electronics", 1200.00),
            ("Headphones", "Electronics", 150.00),
            ("Coffee Maker", "Home", 85.00),
            ("Running Shoes", "Sports", 120.00),
            ("Backpack", "Accessories", 65.00),
            ("Water Bottle", "Sports", 25.00),
            ("Desk Lamp", "Home", 45.00),
            ("Mouse", "Electronics", 35.00),
            ("Yoga Mat", "Sports", 40.00),
            ("Notebook", "Accessories", 15.00),
        ]
        
        customers = [f"Customer_{i:03d}" for i in range(1, 21)]
        cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
                  "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
        
        data = []
        run_timestamp = datetime.now()
        
        for i in range(n_orders):
            product = random.choice(products)
            quantity = random.randint(1, 5)
            unit_price = product[2]
            discount = random.choice([0, 0, 0, 0.1, 0.15, 0.2])  # 50% chance of no discount
            
            order = {
                "order_id": f"ORD{run_timestamp.strftime('%Y%m%d%H%M%S')}{i:04d}",
                "customer_name": random.choice(customers),
                "product_name": product[0],
                "category": product[1],
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_percent": discount * 100,
                "city": random.choice(cities),
                "order_timestamp": run_timestamp,
                "dag_run_date": execution_date,
            }
            data.append(order)
        
        df = pd.DataFrame(data)
        print(f"✅ Generated {len(df)} orders")
        print(df.head())
        
        # Convert Timestamp to string for JSON serialization (Airflow XCom requirement)
        df['order_timestamp'] = df['order_timestamp'].astype(str)
        
        # Convert to dict for XCom serialization
        return df.to_dict(orient='records')
    
    @task
    def transform_data(order_data: list) -> list:
        """
        Transform: Clean and enrich the order data
        - Calculate total amount (after discount)
        - Add order status
        - Categorize order value
        """
        print("🔄 Starting data transformation...")
        
        df = pd.DataFrame(order_data)
        
        # Calculate total amount after discount
        df['subtotal'] = df['quantity'] * df['unit_price']
        df['discount_amount'] = df['subtotal'] * (df['discount_percent'] / 100)
        df['total_amount'] = df['subtotal'] - df['discount_amount']
        
        # Add order status (most are completed, some pending)
        df['order_status'] = np.random.choice(
            ['completed', 'completed', 'completed', 'completed', 'pending', 'shipped'],
            size=len(df)
        )
        
        # Categorize order value
        def categorize_order_value(amount):
            if amount < 50:
                return 'small'
            elif amount < 150:
                return 'medium'
            else:
                return 'large'
        
        df['order_size_category'] = df['total_amount'].apply(categorize_order_value)
        
        # Round numeric columns for cleaner display
        df['unit_price'] = df['unit_price'].round(2)
        df['subtotal'] = df['subtotal'].round(2)
        df['discount_amount'] = df['discount_amount'].round(2)
        df['total_amount'] = df['total_amount'].round(2)
        
        print(f"✅ Transformed {len(df)} orders")
        print("\nSample of transformed data:")
        print(df[['order_id', 'customer_name', 'product_name', 'total_amount', 'order_status']].head())
        print(f"\nTotal revenue in this batch: ${df['total_amount'].sum():.2f}")
        
        return df.to_dict(orient='records')
    
    @task
    def load_to_postgres(transformed_data: list):
        """
        Load: Insert the transformed data into PostgreSQL
        Creates table if it doesn't exist, otherwise appends data
        """
        print("💾 Starting data load to PostgreSQL...")
        
        df = pd.DataFrame(transformed_data)
        
        # Connect to PostgreSQL using Airflow connection
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Check if table exists
        check_table_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'ecommerce_orders'
        );
        """
        
        table_exists = pg_hook.get_first(check_table_sql)[0]
        
        if not table_exists:
            print("📋 Table 'ecommerce_orders' does not exist. Creating...")
            
            # Create table
            create_table_sql = """
            CREATE TABLE ecommerce_orders (
                id SERIAL PRIMARY KEY,
                order_id VARCHAR(50) UNIQUE NOT NULL,
                customer_name VARCHAR(100),
                product_name VARCHAR(100),
                category VARCHAR(50),
                quantity INTEGER,
                unit_price DECIMAL(10, 2),
                discount_percent DECIMAL(5, 2),
                city VARCHAR(50),
                order_timestamp TIMESTAMP,
                dag_run_date DATE,
                subtotal DECIMAL(10, 2),
                discount_amount DECIMAL(10, 2),
                total_amount DECIMAL(10, 2),
                order_status VARCHAR(20),
                order_size_category VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            pg_hook.run(create_table_sql)
            print("✅ Table created successfully")
        else:
            print("📋 Table 'ecommerce_orders' already exists. Appending data...")
        
        # Insert data
        # Convert DataFrame to list of tuples for insertion
        columns = ['order_id', 'customer_name', 'product_name', 'category', 'quantity', 
                   'unit_price', 'discount_percent', 'city', 'order_timestamp', 
                   'dag_run_date', 'subtotal', 'discount_amount', 'total_amount', 
                   'order_status', 'order_size_category']
        
        values = df[columns].values.tolist()
        
        # Use insert_rows for batch insert
        pg_hook.insert_rows(
            table='ecommerce_orders',
            rows=values,
            target_fields=columns,
            replace=False,
            commit_every=1000
        )
        
        print(f"✅ Successfully inserted {len(df)} records into PostgreSQL")
        
        # Get total count
        count_sql = "SELECT COUNT(*) FROM ecommerce_orders;"
        total_records = pg_hook.get_first(count_sql)[0]
        print(f"📊 Total records in ecommerce_orders table: {total_records}")
        
        # Get summary statistics
        summary_sql = """
        SELECT 
            COUNT(*) as total_orders,
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_order_value,
            COUNT(DISTINCT customer_name) as unique_customers
        FROM ecommerce_orders;
        """
        
        summary = pg_hook.get_first(summary_sql)
        print(f"\n📈 Summary Statistics:")
        print(f"   Total Orders: {summary[0]}")
        print(f"   Total Revenue: ${summary[1]:.2f}")
        print(f"   Average Order Value: ${summary[2]:.2f}")
        print(f"   Unique Customers: {summary[3]}")
        
        return f"Loaded {len(df)} records. Total table size: {total_records}"
    
    # Define the task flow
    raw_data = extract_generate_data()
    transformed = transform_data(raw_data)
    load_result = load_to_postgres(transformed)
    
    # Set dependencies: extract -> transform -> load
    raw_data >> transformed >> load_result


# Instantiate the DAG
ecommerce_etl_dag = ecommerce_etl()
