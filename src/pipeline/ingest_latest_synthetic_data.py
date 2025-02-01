import os
import pandas as pd
import boto3
import json
from sqlalchemy import create_engine
from dotenv import load_dotenv
import psycopg2
import tempfile
from pandas import json_normalize
from datetime import datetime
import uuid
import io
import duckdb

load_dotenv()

class IncrementalETL:
    def __init__(self, motherduck_token=None):
        # Initialize connections
        self._init_postgres()
        self._init_s3()
        self._init_duckdb()
        # self._init_motherduck(motherduck_token)
        
        # Store token for later use after database reset
        self.motherduck_token = motherduck_token or os.getenv('MOTHERDUCK_TOKEN')
        if not self.motherduck_token:
            raise ValueError("MotherDuck token not provided")
        
        
        # Define source mappings
        self.postgres_tables = ['categories', 'subcategories', 'order_items', 'interactions']
        self.s3_tables = ['customers', 'products', 'orders', 'reviews']
        
        # Track batch information
        self.batch_id = str(uuid.uuid4())
        self.batch_timestamp = datetime.now()
        
    def _init_postgres(self):
        """Initialize PostgreSQL connection"""
        self.pg_conn_string = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        self.pg_engine = create_engine(self.pg_conn_string)

    def _init_s3(self):
        """Initialize S3 clients"""
        s3_credentials = {
            'aws_access_key_id': os.getenv('AWS_S3_ACCESS_KEY_ID'),
            'aws_secret_access_key': os.getenv('AWS_S3_SECRET_ACCESS_KEY')
        }
        
        self.s3_client = boto3.client('s3', **s3_credentials)
        self.historic_bucket = os.getenv('AWS_S3_HISTORIC_SYNTH')
        self.latest_bucket = os.getenv('AWS_S3_LATEST_SYNTH')

    def _init_duckdb(self):
        """Initialize local DuckDB connection"""
        try:
            # Connect to local DuckDB file
            self.duck_conn = duckdb.connect('ecom_db')
            
            # Create schema if it doesn't exist
            self.duck_conn.execute("CREATE SCHEMA IF NOT EXISTS ecom_raw")
            
            print("Successfully connected to local DuckDB")
            
        except Exception as e:
            print(f"Local DuckDB initialization error: {str(e)}")
            raise

    def _init_motherduck(self, token):
        """Initialize MotherDuck connection"""
        try:
            # Use token from parameter or environment variable
            md_token = token or os.getenv('MOTHERDUCK_TOKEN')
            if not md_token:
                raise ValueError("MotherDuck token not provided")

            # First connect to MotherDuck without specifying a database
            connection_string = f"md:?motherduck_token={md_token}"
            self.md_conn = duckdb.connect(connection_string)
            
            # Create database if it doesn't exist
            self.md_conn.execute("CREATE DATABASE IF NOT EXISTS ecom_db")
            
            # Close initial connection
            self.md_conn.close()
            
            # Now connect specifically to the ecom_db database
            connection_string = f"md:ecom_db?motherduck_token={md_token}"
            self.md_conn = duckdb.connect(connection_string)
            
            # Create schema if it doesn't exist
            self.md_conn.execute("CREATE SCHEMA IF NOT EXISTS ecom_raw")
            
            print("Successfully connected to MotherDuck")
            
        except Exception as e:
            print(f"MotherDuck initialization error: {str(e)}")
            raise

    def extract_historic_from_s3(self, table_name):
        """Extract historic data from S3 CSV files"""
        try:
            # Get CSV file from historic bucket
            response = self.s3_client.get_object(
                Bucket=self.historic_bucket,
                Key=f'csv/{table_name}.csv'
            )
            
            # Read CSV content
            csv_content = response['Body'].read().decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_content))
            df.columns = [col.upper() for col in df.columns]
            return df
            
        except Exception as e:
            print(f"Historic S3 extraction error for {table_name}: {str(e)}")
            raise

    def extract_latest_from_postgres(self, table_name):
        """Extract latest data from PostgreSQL"""
        try:
            query = f"SELECT * FROM latest_{table_name}"
            df = pd.read_sql(query, self.pg_engine)
            df.columns = [col.upper() for col in df.columns]
            return df
        except Exception as e:
            print(f"PostgreSQL extraction error for {table_name}: {str(e)}")
            raise

    def extract_latest_from_s3(self, table_name):
        """Extract latest data from S3"""
        try:
            response = self.s3_client.get_object(
                Bucket=self.latest_bucket,
                Key=f'json/{table_name}.json'
            )
            
            json_content = json.loads(response['Body'].read().decode('utf-8'))
            df = pd.DataFrame(json_content['data'])
            df.columns = [col.upper() for col in df.columns]
            return df
        
        except Exception as e:
            print(f"Latest S3 extraction error for {table_name}: {str(e)}")
            raise
        
    def get_primary_keys(self, table_name):
        """Return primary key columns for each table"""
        pk_mapping = {
            'CUSTOMERS': ['CUSTOMER_ID'],
            'ORDERS': ['ORDER_ID'],
            'PRODUCTS': ['PRODUCT_ID'],
            'ORDER_ITEMS': ['ORDER_ITEM_ID'],
            'CATEGORIES': ['CATEGORY_ID'],
            'SUBCATEGORIES': ['SUBCATEGORY_ID'],
            'REVIEWS': ['REVIEW_ID'],
            'INTERACTIONS': ['EVENT_ID']  # Changed from 'id'
        }
        clean_table = table_name.upper().replace('LATEST_', '')
        return pk_mapping.get(clean_table, ['EVENT_ID' if 'INTERACTIONS' in clean_table else 'id'])

    def remove_duplicate_primary_keys(self, df, table_name):
        """Remove rows with duplicate primary keys, handling missing keys"""
        try:
            # Get primary keys for the table
            primary_keys = self.get_primary_keys(table_name)
            
            if not primary_keys or primary_keys == ['id']:
                # For interactions table, use EVENT_ID as primary key
                if 'EVENT_ID' in df.columns:
                    primary_keys = ['EVENT_ID']
                else:
                    print(f"Warning: No valid primary keys found for {table_name}. Skipping deduplication.")
                    return df

            # Ensure all primary keys exist in the DataFrame
            missing_keys = [key for key in primary_keys if key not in df.columns]
            if missing_keys:
                print(f"Warning: Missing primary keys {missing_keys} in table {table_name}.")
                return df

            # Sort by LOADED_AT if exists, keeping most recent
            if 'LOADED_AT' in df.columns:
                df['LOADED_AT'] = pd.to_datetime(df['LOADED_AT'], errors='coerce')
                df = df.sort_values('LOADED_AT', ascending=False)

            # Deduplicate based on primary keys
            df = df.drop_duplicates(subset=primary_keys, keep='first')
            
            return df
        except Exception as e:
            print(f"Error removing duplicate primary keys for {table_name}: {str(e)}")
            raise
    
    def transform_data(self, df, table_name):
        """Transform data with enhanced column handling and type conversion"""
        try:
            print(f"\nDETAILED COLUMN ANALYSIS FOR {table_name}")
            print("=" * 50)
            
            # Force column names to uppercase first
            df.columns = [col.upper() for col in df.columns]
            
            # Handle reviews table special case - add REVIEW_ID if missing
            if table_name.upper().startswith('REVIEWS') and 'REVIEW_ID' not in df.columns:
                df['REVIEW_ID'] = range(1, len(df) + 1)
            
            # Convert DATA_SOURCE to string type explicitly
            if 'DATA_SOURCE' in df.columns:
                df['DATA_SOURCE'] = df['DATA_SOURCE'].astype(str)
                
            # Handle datetime columns before adding metadata
            date_columns = df.select_dtypes(include=['datetime64']).columns
            for col in date_columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                except Exception as e:
                    print(f"Warning: Error converting datetime column {col}: {str(e)}")
            
            # Add metadata columns with explicit types
            df['DATA_SOURCE'] = df.get('DATA_SOURCE', 'historic')
            df['BATCH_ID'] = df.get('BATCH_ID', str(self.batch_id))
            df['LOADED_AT'] = df.get('LOADED_AT', self.batch_timestamp.strftime('%Y-%m-%d %H:%M:%S'))
            
            # Handle NA/NaT values
            df = df.replace({pd.NA: None, pd.NaT: None})
            
            # Convert types based on expected schema
            df = self.convert_column_types(df, table_name)
            
            # Remove duplicate rows based on primary keys
            df = self.remove_duplicate_primary_keys(df, table_name)
            
            print("\nFinal columns with types:")
            for col in df.columns:
                print(f"- {col}: {df[col].dtype}")
            
            return df
                
        except Exception as e:
            print(f"Transform error for {table_name}: {str(e)}")
            raise

    def flatten_json_df(self, df, table_name):
        """Flatten nested JSON structures"""
        try:
            json_columns = [
                col for col in df.columns 
                if df[col].dtype == 'object' and 
                isinstance(df[col].dropna().iloc[0] if not df[col].isna().all() else None, (dict, list))
            ]
            
            if not json_columns:
                return df

            flat_df = df.copy()
            
            for col in json_columns:
                try:
                    if isinstance(df[col].dropna().iloc[0], dict):
                        flattened = pd.json_normalize(df[col].dropna(), sep='_')
                        flat_df = flat_df.drop(columns=[col])
                        for new_col in flattened.columns:
                            flat_df[f"{col}_{new_col}"] = flattened[new_col]
                    elif isinstance(df[col].dropna().iloc[0], list):
                        flat_df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)
                except Exception as e:
                    print(f"Warning: Could not flatten column {col}: {str(e)}")
                    continue
            
            return flat_df
        except Exception as e:
            print(f"JSON flattening error for {table_name}: {str(e)}")
            raise

    def save_to_s3_historic(self, df, table_name, metadata):
        """Save transformed data and metadata to S3 historic bucket"""
        try:
            # Save as CSV
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            
            # Save to historic data folder
            self.s3_client.put_object(
                Bucket=self.historic_bucket,
                Key=f'csv/{table_name}.csv',
                Body=csv_buffer.getvalue().encode('utf-8')
            )
            
            # Save metadata
            self.s3_client.put_object(
                Bucket=self.historic_bucket,
                Key=f'csv/{table_name}_metadata.json',
                Body=json.dumps(metadata, default=str)
            )
            
            print(f"Successfully saved {table_name} to historic bucket as CSV with metadata")
            
        except Exception as e:
            print(f"Error saving to S3 historic bucket: {str(e)}")
            raise
        
    def get_column_types(self, table_name):
        """Define expected column types for each table"""
        type_mappings = {
            'REVIEWS': {
                'PRODUCT_ID': 'VARCHAR',
                'ORDER_ID': 'VARCHAR',
                'CUSTOMER_ID': 'VARCHAR',
                'REVIEW_ID': 'VARCHAR',
                'REVIEW_SCORE': 'BIGINT',
                'REVIEW_TEXT': 'VARCHAR',
                'DATA_SOURCE': 'VARCHAR',
                'BATCH_ID': 'VARCHAR',
                'LOADED_AT': 'TIMESTAMP'
            },
            'ORDERS': {
                'ORDER_ID': 'VARCHAR',
                'CUSTOMER_ID': 'VARCHAR',
                'STATUS': 'VARCHAR',
                'TOTAL_AMOUNT': 'DOUBLE',
                'SHIPPING_COST': 'DOUBLE',
                'PAYMENT_METHOD': 'VARCHAR',
                'SHIPPING_ADDRESS': 'VARCHAR',
                'BILLING_ADDRESS': 'VARCHAR',
                'ORDER_DATE': 'TIMESTAMP',
                'CREATED_AT': 'TIMESTAMP',
                'UPDATED_AT': 'TIMESTAMP',
                'DATA_SOURCE': 'VARCHAR',
                'BATCH_ID': 'VARCHAR',
                'LOADED_AT': 'TIMESTAMP'
            },
            'ORDER_ITEMS': {
                'ORDER_ITEM_ID': 'VARCHAR',
                'ORDER_ID': 'VARCHAR',
                'PRODUCT_ID': 'VARCHAR',
                'QUANTITY': 'BIGINT',
                'UNIT_PRICE': 'DOUBLE',
                'TOTAL_PRICE': 'DOUBLE',
                'CREATED_AT': 'TIMESTAMP',
                'DATA_SOURCE': 'VARCHAR',
                'BATCH_ID': 'VARCHAR',
                'LOADED_AT': 'TIMESTAMP'
            },
            'PRODUCTS': {
                'PRODUCT_ID': 'VARCHAR',
                'CATEGORY_ID': 'VARCHAR',
                'SUBCATEGORY_ID': 'VARCHAR',
                'PRODUCT_NAME': 'VARCHAR',
                'DESCRIPTION': 'VARCHAR',
                'BASE_PRICE': 'DOUBLE',
                'SALE_PRICE': 'DOUBLE',
                'STOCK_QUANTITY': 'BIGINT',
                'WEIGHT_KG': 'DOUBLE',
                'IS_ACTIVE': 'BOOLEAN',
                'BRAND': 'VARCHAR',
                'SKU': 'VARCHAR',
                'RATING': 'DOUBLE',
                'REVIEW_COUNT': 'BIGINT',
                'CREATED_AT': 'TIMESTAMP',
                'DATA_SOURCE': 'VARCHAR',
                'BATCH_ID': 'VARCHAR',
                'LOADED_AT': 'TIMESTAMP'
            },
            'CUSTOMERS': {
                'CUSTOMER_ID': 'VARCHAR',
                'EMAIL': 'VARCHAR',
                'FIRST_NAME': 'VARCHAR',
                'LAST_NAME': 'VARCHAR',
                'AGE': 'BIGINT',
                'GENDER': 'VARCHAR',
                'ANNUAL_INCOME': 'DOUBLE',
                'MARITAL_STATUS': 'VARCHAR',
                'EDUCATION': 'VARCHAR',
                'LOCATION_TYPE': 'VARCHAR',
                'CITY': 'VARCHAR',
                'STATE': 'VARCHAR',
                'COUNTRY': 'VARCHAR',
                'SIGNUP_DATE': 'TIMESTAMP',
                'LAST_LOGIN': 'TIMESTAMP',
                'PREFERRED_CHANNEL': 'VARCHAR',
                'IS_ACTIVE': 'BOOLEAN',
                'DATA_SOURCE': 'VARCHAR',
                'BATCH_ID': 'VARCHAR',
                'LOADED_AT': 'TIMESTAMP'
            }
        }
        return type_mappings.get(table_name.upper().replace('LATEST_', ''), {})

    def convert_column_types(self, df, table_name):
        """Convert DataFrame columns to appropriate types"""
        expected_types = self.get_column_types(table_name)
        
        if not expected_types:
            return df  # Return unchanged if no type mapping exists
        
        for column in df.columns:
            if column in expected_types:
                try:
                    dtype = expected_types[column]
                    if dtype == 'BIGINT':
                        df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype('int64')
                    elif dtype == 'DOUBLE':
                        df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0.0)
                    elif dtype == 'VARCHAR':
                        # Special handling for ID columns
                        if column.endswith('_ID'):
                            df[column] = df[column].fillna('').astype(str)
                        else:
                            df[column] = df[column].fillna('').astype(str)
                    elif dtype == 'BOOLEAN':
                        df[column] = df[column].fillna(False).astype(bool)
                    elif dtype == 'TIMESTAMP':
                        df[column] = pd.to_datetime(df[column], errors='coerce')
                        df[column] = df[column].dt.strftime('%Y-%m-%d %H:%M:%S')
                except Exception as e:
                    print(f"Warning: Error converting column {column} to {dtype}: {str(e)}")
                    # Keep the original data if conversion fails
                    continue
        
        return df

    def get_duck_type(self, dtype, column_name):
        """Determine DuckDB column type based on pandas dtype and column name"""
        # Always use VARCHAR for ID columns
        if column_name.upper().endswith('_ID'):
            return 'VARCHAR'
        
        # Handle other types
        if pd.api.types.is_integer_dtype(dtype):
            return 'BIGINT'
        elif pd.api.types.is_float_dtype(dtype):
            return 'DOUBLE'
        elif pd.api.types.is_bool_dtype(dtype):
            return 'BOOLEAN'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return 'TIMESTAMP'
        else:
            return 'VARCHAR'

    def load_to_motherduck(self, df, table_name):
        """Load data to MotherDuck with type handling"""
        try:
            full_table_name = f"ecom_raw.{table_name.lower()}"
            
            # Force string types for metadata columns
            metadata_columns = ['DATA_SOURCE', 'BATCH_ID']
            for col in metadata_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str)
            
            # Get column definitions with explicit types
            column_definitions = []
            for col, dtype in df.dtypes.items():
                col_type = self.get_duck_type(dtype, col)
                if col in metadata_columns:
                    col_type = 'VARCHAR'  # Force VARCHAR for metadata columns
                column_definitions.append(f'"{col}" {col_type}')
            
            # Create target table if it doesn't exist
            self.duck_conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {full_table_name} (
                    {', '.join(column_definitions)}
                )
            """)
            
            # Get primary keys
            primary_keys = [pk.upper() for pk in self.get_primary_keys(table_name)]
            
            # Create view of new data
            self.duck_conn.execute("CREATE TEMPORARY VIEW new_data AS SELECT * FROM df")
            
            # Delete existing records that will be updated
            if primary_keys:
                delete_condition = " AND ".join([
                    f"{full_table_name}.\"{pk}\" IN (SELECT \"{pk}\" FROM df)"
                    for pk in primary_keys
                ])
                
                self.duck_conn.execute(f"""
                    DELETE FROM {full_table_name}
                    WHERE {delete_condition}
                """)
            
            # Insert new records directly from DataFrame
            self.duck_conn.execute(f"""
                INSERT INTO {full_table_name}
                SELECT * FROM df
            """)
            
            # Get stats
            row_count = self.duck_conn.execute(f"""
                SELECT COUNT(*) FROM {full_table_name}
            """).fetchone()[0]
            
            print(f"Successfully loaded data to MotherDuck {full_table_name}:")
            print(f"- Rows in table: {row_count}")
            
        except Exception as e:
            print(f"Error loading to MotherDuck: {str(e)}")
            raise

    def load_to_duckdb(self, df, table_name):
        """Load data to local DuckDB with consistent type handling"""
        try:
            full_table_name = f"ecom_raw.{table_name.lower()}"
            
            # Force all ID columns to VARCHAR/string type
            for col in df.columns:
                if col.upper().endswith('_ID'):
                    df[col] = df[col].astype(str)
            
            # Force string types for metadata columns
            metadata_columns = ['DATA_SOURCE', 'BATCH_ID']
            for col in metadata_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str)
            
            # Get column definitions with explicit types
            column_definitions = []
            for col, dtype in df.dtypes.items():
                col_type = self.get_duck_type(dtype, col)
                if col.upper().endswith('_ID') or col in metadata_columns:
                    col_type = 'VARCHAR'  # Force VARCHAR for ID and metadata columns
                column_definitions.append(f'"{col}" {col_type}')
            
            # Create target table if it doesn't exist
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {full_table_name} (
                    {', '.join(column_definitions)}
                )
            """
            self.duck_conn.execute(create_table_sql)
            
            # Get primary keys
            primary_keys = [pk.upper() for pk in self.get_primary_keys(table_name)]
            
            # Create temporary table for new data with same schema
            temp_table_name = f"temp_{table_name.lower()}"
            self.duck_conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            self.duck_conn.execute(f"""
                CREATE TEMPORARY TABLE {temp_table_name} AS 
                SELECT * FROM {full_table_name} WHERE 1=0
            """)
            
            # Insert data into temporary table
            self.duck_conn.execute(f"""
                INSERT INTO {temp_table_name}
                SELECT * FROM df
            """)
            
            # Delete existing records that will be updated
            if primary_keys:
                pk_conditions = []
                for pk in primary_keys:
                    pk_conditions.append(
                        f"{full_table_name}.\"{pk}\"::VARCHAR IN (SELECT \"{pk}\"::VARCHAR FROM {temp_table_name})"
                    )
                delete_condition = " AND ".join(pk_conditions)
                
                self.duck_conn.execute(f"""
                    DELETE FROM {full_table_name}
                    WHERE {delete_condition}
                """)
            
            # Insert new records from temporary table
            self.duck_conn.execute(f"""
                INSERT INTO {full_table_name}
                SELECT * FROM {temp_table_name}
            """)
            
            # Clean up temporary table
            self.duck_conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            
            # Get stats
            row_count = self.duck_conn.execute(f"""
                SELECT COUNT(*) FROM {full_table_name}
            """).fetchone()[0]
            
            print(f"Successfully loaded data to local DuckDB {full_table_name}:")
            print(f"- Rows in table: {row_count}")
            
            return full_table_name
            
        except Exception as e:
            print(f"Error loading to DuckDB: {str(e)}")
            raise

    def cleanup_temp_tables(self):
        """Clean up any temporary tables that might exist"""
        try:
            # Get list of temporary tables
            temp_tables = self.duck_conn.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'ecom_raw' 
                AND table_name LIKE 'temp_%'
            """).fetchall()
            
            # Drop each temporary table
            for table in temp_tables:
                self.duck_conn.execute(f"DROP TABLE IF EXISTS ecom_raw.{table[0]}")
                print(f"Dropped temporary table: {table[0]}")
                
        except Exception as e:
            print(f"Error cleaning up temporary tables: {str(e)}")

    def sync_to_motherduck(self, table_name):
        """Sync local DuckDB table to MotherDuck with improved cross-database query handling"""
        try:
            full_table_name = f"ecom_raw.{table_name.lower()}"
            
            # Get the schema from local DuckDB
            schema_query = f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{table_name.lower()}'
            AND table_schema = 'ecom_raw'
            ORDER BY ordinal_position
            """
            columns = self.duck_conn.execute(schema_query).fetchall()
            
            # Build CREATE TABLE statement
            column_definitions = [f'"{col[0]}" {col[1]}' for col in columns]
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                {', '.join(column_definitions)}
            )
            """
            
            # Create table in MotherDuck
            self.md_conn.execute(create_table_sql)
            
            # Get row count from local table
            local_count = self.duck_conn.execute(f"SELECT COUNT(*) FROM {full_table_name}").fetchone()[0]
            
            if local_count > 0:
                # Get primary keys
                primary_keys = [pk.upper() for pk in self.get_primary_keys(table_name)]
                
                # Get the data from local DuckDB as a DataFrame
                local_data = self.duck_conn.execute(f"SELECT * FROM {full_table_name}").df()
                
                # If table has primary keys, use merge approach
                if primary_keys:
                    # Create temporary table in MotherDuck
                    temp_table = f"temp_{table_name.lower()}"
                    self.md_conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
                    
                    # Create temp table with same schema
                    self.md_conn.execute(f"""
                        CREATE TEMPORARY TABLE {temp_table} (
                            {', '.join(column_definitions)}
                        )
                    """)
                    
                    # Insert data into temp table using DataFrame
                    self.md_conn.execute(f"INSERT INTO {temp_table} SELECT * FROM local_data")
                    
                    # Delete existing records that will be updated
                    pk_conditions = []
                    for pk in primary_keys:
                        pk_conditions.append(
                            f"target.\"{pk}\"::VARCHAR IN (SELECT \"{pk}\"::VARCHAR FROM {temp_table})"
                        )
                    delete_condition = " AND ".join(pk_conditions)
                    
                    self.md_conn.execute(f"""
                        DELETE FROM {full_table_name} target
                        WHERE {delete_condition}
                    """)
                    
                    # Insert new records
                    self.md_conn.execute(f"""
                        INSERT INTO {full_table_name}
                        SELECT * FROM {temp_table}
                    """)
                    
                    # Clean up temp table
                    self.md_conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
                
                else:
                    # For tables without primary keys, just append using DataFrame
                    self.md_conn.execute(f"INSERT INTO {full_table_name} SELECT * FROM local_data")
            
            # Verify row count
            md_count = self.md_conn.execute(f"SELECT COUNT(*) FROM {full_table_name}").fetchone()[0]
            
            print(f"Successfully synced to MotherDuck {full_table_name}:")
            print(f"- Local rows: {local_count}")
            print(f"- MotherDuck rows: {md_count}")
            
            if local_count != md_count:
                print(f"Warning: Row count mismatch between local ({local_count}) and MotherDuck ({md_count})")
            
        except Exception as e:
            print(f"Error syncing to MotherDuck: {str(e)}")
            print("Full error details:")
            import traceback
            print(traceback.format_exc())
            raise

    def reset_motherduck_database(self):
        """Delete and recreate the MotherDuck database"""
        try:
            # First connect without database to drop/create it
            print("\nResetting MotherDuck database...")
            connection_string = f"md:?motherduck_token={self.motherduck_token}"
            temp_conn = duckdb.connect(connection_string)
            
            try:
                # Drop database if exists
                temp_conn.execute("DROP DATABASE IF EXISTS ecom_db")
                print("Existing database dropped successfully")
                
                # Create fresh database
                temp_conn.execute("CREATE DATABASE ecom_db")
                print("New database created successfully")
                
            finally:
                temp_conn.close()
            
            # Connect to new database
            connection_string = f"md:ecom_db?motherduck_token={self.motherduck_token}"
            self.md_conn = duckdb.connect(connection_string)
            
            # Create schema
            self.md_conn.execute("CREATE SCHEMA IF NOT EXISTS ecom_raw")
            print("Schema created successfully")
            
            return True
            
        except Exception as e:
            print(f"Error resetting MotherDuck database: {str(e)}")
            raise

    def reset_local_duckdb(self):
        """Reset local DuckDB database"""
        try:
            print("\nResetting local DuckDB...")
            
            # Ensure existing connection is closed
            if hasattr(self, 'duck_conn'):
                self.duck_conn.close()

            
            # Try to remove the file, with error handling
            try:
                if os.path.exists('ecom_db'):
                    os.remove('ecom_db')
                    print("Existing local database file removed")
            except PermissionError:
                print("Could not remove ecom_db. Attempting alternative cleanup.")
                
                # Alternative approach: use duckdb to detach and remove
                try:
                    # Forcefully close any existing connections
                    duckdb.close_all_connections()
                    
                    # Wait a moment to ensure connections are closed
                    import time
                    time.sleep(1)
                    
                    # Try removing again
                    if os.path.exists('ecom_db'):
                        os.remove('ecom_db')
                        print("Database file removed successfully after connection closure")
                except Exception as e:
                    print(f"Failed to remove database file: {e}")
                    raise
            
            # Create fresh connection
            self.duck_conn = duckdb.connect('ecom_db')
            
            # Create schema
            self.duck_conn.execute("CREATE SCHEMA IF NOT EXISTS ecom_raw")
            print("Local database reset successfully")
            
            return True
            
        except Exception as e:
            print(f"Error resetting local DuckDB: {str(e)}")
            raise

    def run_etl(self):
        """Execute ETL process with database reset"""
        try:
            print("\nStarting ETL process with database reset...")
            
            # Reset both databases
            self.reset_local_duckdb()
            self.reset_motherduck_database()
            
            for table in self.postgres_tables + self.s3_tables:
                print(f"\nProcessing {table}")
                
                try:
                    # Extract data
                    historic_df = self.extract_historic_from_s3(table)
                    latest_df = (self.extract_latest_from_s3(table)
                            if table in self.s3_tables 
                            else self.extract_latest_from_postgres(table))
                    
                    # Transform data
                    historic_df = self.transform_data(historic_df, f"{table}")
                    latest_df = self.transform_data(latest_df, f"latest_{table}")
                    
                    # Combine datasets
                    all_columns = list(set(historic_df.columns) | set(latest_df.columns))
                    for col in all_columns:
                        if col not in historic_df.columns:
                            historic_df[col] = None
                        if col not in latest_df.columns:
                            latest_df[col] = None
                    
                    historic_df = historic_df[all_columns]
                    latest_df = latest_df[all_columns]
                    
                    combined_df = pd.concat([historic_df, latest_df], ignore_index=True)
                    transformed_df = self.transform_data(combined_df, table)
                    
                    # Load data
                    print(f"\nLoading {table} to databases...")
                    self.load_to_duckdb(transformed_df, table)
                    self.sync_to_motherduck(table)
                    
                    print(f"Successfully processed {table}")
                    
                except Exception as e:
                    print(f"Error processing table {table}: {str(e)}")
                    continue
                    
        except Exception as e:
            print(f"ETL process error: {str(e)}")
            raise
        finally:
            # Clean up connections
            if hasattr(self, 'duck_conn'):
                self.duck_conn.close()
            if hasattr(self, 'md_conn'):
                self.md_conn.close()
            print("\nETL process completed. All connections closed.")
            
if __name__ == "__main__":
    try:
        # Get MotherDuck token from environment variable
        token = os.getenv('MOTHERDUCK_TOKEN')
        etl = IncrementalETL(token)
        etl.run_etl()
    except Exception as e:
        print(f"Main execution error: {str(e)}")
        raise