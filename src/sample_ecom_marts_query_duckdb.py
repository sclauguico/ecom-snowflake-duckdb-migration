import duckdb

# Connect to the database
conn = duckdb.connect('ecom_db')

# List schemas
print("Schemas:")
schemas = conn.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
print(schemas)

# List tables in ecom_marts schema
print("\nTables in ecom_marts schema:")
tables = conn.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'ecom_marts'
""").fetchall()
print(tables)

# Check columns in fct_customer_activity table
print("\nColumns in fct_customer_activity table:")
columns = conn.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema = 'ecom_marts' AND table_name = 'fct_customer_activity'
""").fetchall()
print(columns)

# Verify table existence and row count
print("\nTable existence and row count:")
try:
    row_count = conn.execute("SELECT COUNT(*) FROM ecom_marts.fct_customer_activity").fetchone()[0]
    print(f"fct_customer_activity table exists. Row count: {row_count}")
except Exception as e:
    print(f"Error checking fct_customer_activity table: {e}")

# Print first few rows if table exists
print("\nFirst 5 rows:")
try:
    first_rows = conn.execute("SELECT * FROM ecom_marts.fct_customer_activity LIMIT 5").fetchall()
    print(first_rows)
except Exception as e:
    print(f"Error fetching rows: {e}")