# Airflow Learning Project - Quick Reference

## Project Overview
This is a local Airflow development environment using Astro CLI with an E-commerce ETL pipeline DAG.

## Key Commands

### Start/Restart Environment
```bash
astro dev start      # Start the environment
astro dev restart    # Restart after code changes
astro dev stop       # Stop without losing data
astro dev kill       # Stop and delete all data
```

### Port Configuration (Changed from 5432 to 5433)
**Issue**: Port conflict with local PostgreSQL
**Solution**: Changed Astro's Postgres port
```bash
astro config set postgres.port 5433
```
**Access**: 
- Airflow UI: http://localhost:8080 (admin/admin)
- Postgres: localhost:5433 (NOT a web browser!)

## Database Access

### Connection Details
| Setting | Value |
|---------|-------|
| Host | `localhost` |
| Port | `5433` |
| Database | `postgres` |
| Username | `postgres` |
| Password | `postgres` |

### Tools to Connect
- **Command Line**: `psql -h localhost -p 5433 -U postgres`
- **GUI Tools**: pgAdmin, DBeaver, or VS Code extensions
- **Note**: Cannot access via web browser - PostgreSQL is a database, not a web server!

## Airflow Connection Setup (CRITICAL!)

### Required: Create Postgres Connection
Before running the ETL DAG, you **MUST** create this connection:

1. Go to Airflow UI → **Admin** → **Connections**
2. Click **+** to add new connection
3. Fill in:
   - **Connection Id**: `postgres_default`
   - **Connection Type**: `Postgres`
   - **Host**: `postgres`
   - **Port**: `5432` (internal Docker port, not 5433!)
   - **Schema**: `postgres`
   - **Login**: `postgres`
   - **Password**: `postgres`
4. Click **Save**

## ETL DAG: ecommerce_etl_pipeline

### What It Does
1. **EXTRACT**: Generates 10-50 random e-commerce orders
2. **TRANSFORM**: Calculates totals, adds status, categorizes orders
3. **LOAD**: Saves to PostgreSQL (creates table first time, appends thereafter)

### Data Flow
```
extract_generate_data → transform_data → load_to_postgres
```

### Key Learnings & Fixes Applied

#### 1. Serialization Error (FIXED)
**Error**: `TypeError: cannot serialize pandas._libs.tslibs.timestamps.Timestamp`
**Problem**: Airflow XCom can't serialize pandas Timestamp objects
**Solution**: Convert Timestamp to string before returning
```python
df['order_timestamp'] = df['order_timestamp'].astype(str)
```

#### 2. Connection Not Found (REQUIRES SETUP)
**Error**: `AirflowNotFoundException: The conn_id 'postgres_default' isn't defined`
**Solution**: Create the connection in Airflow UI (see "Airflow Connection Setup" above)

#### 3. Data Persistence
- Table `ecommerce_orders` is created on first successful run
- Subsequent runs **APPEND** data (don't overwrite)
- Run `astro dev kill` to delete everything and start fresh

## Requirements

File: `requirements.txt`
```
pandas
psycopg2-binary
```

After modifying requirements:
```bash
astro dev restart
```

## Troubleshooting

### Port Already in Use
```bash
astro config set postgres.port 5434  # Try another port
astro config set webserver.port 8081  # If 8080 is taken
```

### Connection Refused Error on Port 5433
**Error**: `OperationalError: connection to server at "postgres" (172.22.0.2), port 5433 failed: Connection refused`

**Problem**: You set the Airflow connection port to `5433`, but that's the **external** port. From inside Docker containers, postgres is on port `5432`.

**Solution**: 
- In Airflow UI → Admin → Connections → `postgres_default`
- Change **Port** from `5433` to **`5432`**
- Save and re-trigger the DAG

**Port Mapping Explanation**:
| Context | Port | When to Use |
|---------|------|-------------|
| **External** (your laptop) | `5433` | pgAdmin, DBeaver, psql from terminal |
| **Internal** (Docker containers) | `5432` | Airflow DAGs connecting to Postgres |

Docker maps: `localhost:5433` (your machine) → `postgres:5432` (container)

### View Logs
```bash
astro dev logs webserver
astro dev logs scheduler
```

### Check Running Containers
```bash
docker ps
```

### Database Queries to Try
```sql
-- View recent orders
SELECT * FROM ecommerce_orders ORDER BY created_at DESC LIMIT 10;

-- Summary by category
SELECT category, COUNT(*) as orders, SUM(total_amount) as revenue 
FROM ecommerce_orders 
GROUP BY category;

-- Total records
SELECT COUNT(*) FROM ecommerce_orders;

-- Run history
SELECT dag_run_date, COUNT(*) as orders_added 
FROM ecommerce_orders 
GROUP BY dag_run_date 
ORDER BY dag_run_date;
```

## File Structure
```
learning-airflow/
├── dags/
│   ├── example_astronauts.py      # Sample DAG (original)
│   └── ecommerce_etl_pipeline.py  # Your ETL DAG
├── requirements.txt               # Python dependencies
├── Dockerfile                     # Astro Runtime image
└── .astro/
    └── config.yaml               # Project config (port settings)
```

## Next Steps
1. ✅ Change Postgres port to avoid conflict
2. ✅ Install pandas & psycopg2-binary in requirements.txt
3. ✅ Fix Timestamp serialization in DAG code
4. ⏳ Create `postgres_default` connection in Airflow UI
5. ⏳ Restart environment: `astro dev restart`
6. ⏳ Trigger DAG from Airflow UI
7. ⏳ Query database to see accumulated data

## Tips
- DAG runs every 5 minutes automatically (or trigger manually)
- Each run generates different random data
- Data accumulates over time (check total record count growing)
- Use database client to query, not web browser
- Connection uses internal Docker port (5432), not external port (5433)
