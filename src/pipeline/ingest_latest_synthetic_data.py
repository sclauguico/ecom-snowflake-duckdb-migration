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
        self._init_motherduck(motherduck_token)
        
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

    def _init_motherduck(self, token):
        """Initialize MotherDuck connection"""
        try:
            # Use token from parameter or environment variable
            md_token = token or os.getenv('MOTHERDUCK_TOKEN')
            if not md_token:
                raise ValueError("MotherDuck token not provided")

            # First connect to MotherDuck's root
            root_connection = f"md:?motherduck_token={md_token}"
            temp_conn = duckdb.connect(root_connection)
            
            # Create database if it doesn't exist
            temp_conn.execute("CREATE DATABASE IF NOT EXISTS ecom_db")
            temp_conn.close()

            # Now connect to the ecom database
            connection_string = f"md:ecom_db?motherduck_token={md_token}"
            self.duck_conn = duckdb.connect(connection_string)
            
            # Create ecom_raw schema if it doesn't exist
            self.duck_conn.execute("CREATE SCHEMA IF NOT EXISTS ecom_raw")
            
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
            'customers': ['CUSTOMER_ID'],
            'orders': ['ORDER_ID'],
            'products': ['PRODUCT_ID'],
            'order_items': ['ORDER_ITEM_ID'],
            'categories': ['CATEGORY_ID'],
            'subcategories': ['SUBCATEGORY_ID'],
            'reviews': ['REVIEW_ID']
        }
        return pk_mapping.get(table_name.replace('latest_', ''), ['id'])

    def remove_duplicate_primary_keys(self, df, table_name):
        """Remove rows with duplicate primary keys, defaulting to the first column if necessary."""
        try:
            # Get primary keys for the table
            primary_keys = self.get_primary_keys(table_name)

            if not primary_keys:
                print(f"Warning: No primary keys found or defined for {table_name}. Skipping deduplication.")
                return df

            # Ensure all primary keys exist in the DataFrame
            missing_keys = [key for key in primary_keys if key not in df.columns]
            if missing_keys:
                print(f"Error: Missing primary keys {missing_keys} in table {table_name}.")
                return df

            # Deduplicate based on primary keys
            if "LOADED_AT" in df.columns:
                df = df.sort_values(by="LOADED_AT", ascending=False)
            else:
                print(f"Warning: No valid date column found for {table_name}. Proceeding without sorting.")
            df = df.drop_duplicates(subset=primary_keys, keep="first")
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
            
            # Handle datetime columns
            date_columns = df.select_dtypes(include=['datetime64']).columns
            for col in date_columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                except Exception as e:
                    print(f"Warning: Error converting datetime column {col}: {str(e)}")
            
            # Add metadata columns with explicit types
            if 'DATA_SOURCE' not in df.columns:
                df['DATA_SOURCE'] = 'historic'
            if 'BATCH_ID' not in df.columns:
                df['BATCH_ID'] = str(self.batch_id)
            if 'LOADED_AT' not in df.columns:
                df['LOADED_AT'] = self.batch_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            
            # Handle NA/NaT values
            df = df.replace({pd.NA: None, pd.NaT: None})
            
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
            temp_table_name = f"ecom_raw.temp_{table_name.lower()}"
            
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
            
            # Create temporary table
            create_temp_table_sql = f"""
            CREATE TABLE {temp_table_name} (
                {', '.join(column_definitions)}
            )
            """
            
            # Drop existing temp table if exists
            self.duck_conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            
            # Create new temp table
            print(f"Creating temp table with columns: {column_definitions}")
            self.duck_conn.execute(create_temp_table_sql)
            
            # Load data into temp table
            self.duck_conn.execute(f"INSERT INTO {temp_table_name} SELECT * FROM df")
            
            # Create target table if it doesn't exist
            self.duck_conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {full_table_name} (
                    {', '.join(column_definitions)}
                )
            """)
            
            # Get primary keys
            primary_keys = [pk.upper() for pk in self.get_primary_keys(table_name)]
            
            # Delete existing records that will be updated
            delete_condition = " AND ".join([
                f"{full_table_name}.\"{pk}\" IN (SELECT \"{pk}\" FROM {temp_table_name})"
                for pk in primary_keys
            ])
            
            self.duck_conn.execute(f"""
                DELETE FROM {full_table_name}
                WHERE {delete_condition}
            """)
            
            # Insert new records
            self.duck_conn.execute(f"""
                INSERT INTO {full_table_name}
                SELECT * FROM {temp_table_name}
            """)
            
            # Get stats
            row_count = self.duck_conn.execute(f"""
                SELECT COUNT(*) FROM {full_table_name}
            """).fetchone()[0]
            
            # Clean up
            self.duck_conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            
            print(f"Successfully loaded data to {full_table_name}:")
            print(f"- Rows in table: {row_count}")
            
        except Exception as e:
            print(f"Error loading to MotherDuck: {str(e)}")
            raise

    def run_etl(self):
        """Execute ETL process"""
        try:
            print("\nStarting ETL process...")
            
            for table in self.postgres_tables + self.s3_tables:
                print(f"\nProcessing {table}")
                
                try:
                    # Extract historic data from S3 CSV
                    historic_df = self.extract_historic_from_s3(table)
                    
                    # Extract latest data from original sources
                    latest_df = (self.extract_latest_from_s3(table)
                            if table in self.s3_tables 
                            else self.extract_latest_from_postgres(table))
                    
                    # Transform each dataset separately first
                    historic_df = self.transform_data(historic_df, f"{table}")
                    latest_df = self.transform_data(latest_df, f"latest_{table}")
                    
                    # Ensure columns match before concatenation
                    all_columns = list(set(historic_df.columns) | set(latest_df.columns))
                    
                    # Add missing columns with NULL values
                    for col in all_columns:
                        if col not in historic_df.columns:
                            historic_df[col] = None
                        if col not in latest_df.columns:
                            latest_df[col] = None
                    
                    # Reorder columns to match
                    historic_df = historic_df[all_columns]
                    latest_df = latest_df[all_columns]
                    
                    # Combine datasets
                    combined_df = pd.concat([historic_df, latest_df], ignore_index=True)
                    
                    # Final transformation on combined data
                    transformed_df = self.transform_data(combined_df, table)
                    
                    # Load to MotherDuck
                    self.load_to_motherduck(transformed_df, table)
                    
                except Exception as e:
                    print(f"Error processing table {table}: {str(e)}")
                    continue
                    
        except Exception as e:
            print(f"ETL process error: {str(e)}")
            raise
        finally:
            self.duck_conn.close()
            print("\nETL process completed. Connections closed.")
            
if __name__ == "__main__":
    try:
        # Get MotherDuck token from environment variable
        token = os.getenv('MOTHERDUCK_TOKEN')
        etl = IncrementalETL(token)
        etl.run_etl()
    except Exception as e:
        print(f"Main execution error: {str(e)}")
        raise