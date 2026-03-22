# Database Connection Setup

## Configuration File Updated

I've configured the `airflow_settings.yaml` file with the **postgres-local** connection that your DAG uses.

### Connection Details:

```yaml
Connection ID: postgres-local
Connection Type: Postgres
Host: postgres          # Docker service name (not localhost)
Port: 5432              # Internal Docker port
Schema: postgres        # Database name
Login: postgres         # Username
Password: postgres      # Password
SSL Mode: disable
```

## How It Works

### Option 1: Using `airflow_settings.yaml` (Recommended for Local Dev)

The `airflow_settings.yaml` file automatically creates connections when you start the Astro environment:

```bash
astro dev start
```

**This will automatically create the `postgres-local` connection** - no manual setup needed!

### Option 2: Manual Connection in Airflow UI

If you prefer to set it up manually:

1. Go to **Admin → Connections** in Airflow UI (http://localhost:8080)
2. Click **+** to add a new connection
3. Fill in:
   - **Connection Id**: `postgres-local`
   - **Connection Type**: `Postgres`
   - **Host**: `postgres` (⚠️ Important: Use service name, not localhost!)
   - **Port**: `5432`
   - **Schema**: `postgres`
   - **Login**: `postgres`
   - **Password**: `postgres`

### Option 3: CLI Command

```bash
astro dev run connections add postgres-local \
  --conn-type postgres \
  --conn-host postgres \
  --conn-port 5432 \
  --conn-schema postgres \
  --conn-login postgres \
  --conn-password postgres
```

## Important Notes

### Docker Networking
- **Host should be `postgres`**, NOT `localhost`!
- In Docker Compose, services communicate using service names as hostnames
- The Astro CLI creates a `postgres` service automatically

### Port Mapping
- **Internal Docker port**: `5432` (what Airflow uses)
- **External host port**: `5433` (what you use from your computer)
- Configured in `.astro/config.yaml`

## Accessing Postgres from Your Local Computer

When you want to connect to the Postgres database from **outside Docker** (from your computer), use these settings:

### Port Configuration Explained

| Context | Host | Port | Why? |
|---------|------|------|------|
| **Inside Docker (Airflow DAGs)** | `postgres` | `5432` | Internal Docker network uses service names |
| **Outside Docker (Your Computer)** | `localhost` | `5433` | Port mapped by Astro CLI in `.astro/config.yaml` |

The port mapping is configured in `.astro/config.yaml`:
```yaml
postgres:
  port: 5433  # Maps internal 5432 → external 5433
```

### Connection Settings for Local Tools

**For DBeaver, pgAdmin, DataGrip, etc.:**
```
Host:     localhost
Port:     5433
Database: postgres
Username: postgres
Password: postgres
SSL:      disable
```

**For psql (Command Line):**
```bash
# Interactive mode
psql -h localhost -p 5433 -U postgres -d postgres

# Run a single command
psql -h localhost -p 5433 -U postgres -d postgres -c "SELECT * FROM stock_data LIMIT 5;"
```

**For Python (from your local machine):**
```python
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="postgres",
    user="postgres",
    password="postgres"
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM stock_data LIMIT 10;")
results = cursor.fetchall()
print(results)

conn.close()
```

### Useful SQL Commands

Once connected, you can run these commands:

```sql
-- List all tables
\dt

-- View table structure
\d stock_data

-- Check how many records exist
SELECT COUNT(*) FROM stock_data;

-- View sample data
SELECT * FROM stock_data LIMIT 10;

-- Check date range in data
SELECT MIN(date), MAX(date) FROM stock_data;

-- View price statistics
SELECT 
    MIN(close_price) as min_price,
    MAX(close_price) as max_price,
    AVG(close_price) as avg_price
FROM stock_data;
```

### Prerequisites

Before connecting locally:
1. **Astro must be running:**
   ```bash
   astro dev start
   ```

2. **Verify Postgres is healthy:**
   ```bash
   astro dev ps
   # Look for: postgres - healthy
   ```

3. **Test the connection:**
   ```bash
   psql -h localhost -p 5433 -U postgres -d postgres -c "SELECT 1;"
   ```

## Verification

After starting Astro:

1. **Check connection exists:**
   ```bash
   astro dev run connections list
   ```

2. **Test connection in Airflow UI:**
   - Go to **Admin → Connections**
   - Find `postgres-local`
   - Click **Test** button

3. **Run your DAG:**
   - The DAG should now be able to connect to Postgres
   - Monitor the `create_table_postgres` task

## Troubleshooting

### Connection Refused Error
If you get connection errors, ensure:
1. Astro is running: `astro dev ps`
2. Postgres container is healthy
3. Connection host is `postgres` (not `localhost`)

### Port Already in Use
If port 5433 is taken, change it in `.astro/config.yaml`:
```yaml
postgres:
  port: 5434  # Use a different port
```

Then update external connections accordingly.

## Production Note

⚠️ **For production deployments**, don't use `airflow_settings.yaml`! 
- Use environment variables
- Or a secrets backend (AWS Secrets Manager, HashiCorp Vault, etc.)
- The password should be rotated and stored securely
