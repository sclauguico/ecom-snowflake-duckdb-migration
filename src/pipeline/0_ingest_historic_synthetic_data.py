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
from pathlib import Path

load_dotenv()

class InitialHistoricETL:
    def __init__(self):
        # Initialize connections
        self._init_postgres()
        self._init_s3()
        
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

    def extract_from_postgres(self, table_name, data_source):
        """Extract data from PostgreSQL"""
        try:
            table_prefix = 'latest_' if data_source == 'latest' else ''
            query = f"SELECT * FROM {table_prefix}{table_name}"
            df = pd.read_sql(query, self.pg_engine)
            return df
        except Exception as e:
            print(f"PostgreSQL extraction error for {table_name}: {str(e)}")
            raise

    def extract_from_s3(self, table_name, data_source):
        """Extract data from S3"""
        try:
            bucket = self.latest_bucket if data_source == 'latest' else self.historic_bucket
            response = self.s3_client.get_object(
                Bucket=bucket,
                Key=f'json/{table_name}.json'
            )
            
            json_content = json.loads(response['Body'].read().decode('utf-8'))
            return pd.DataFrame(json_content['data'])
        except Exception as e:
            print(f"S3 extraction error for {table_name}: {str(e)}")
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
            'reviews': ['REVIEW_ID'],
            'interactions': ['EVENT_ID']
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
        """Transform data with added metadata columns"""
        try:
            # Flatten JSON if needed
            df = self.flatten_json_df(df, table_name)
            
            # Add metadata columns
            df['DATA_SOURCE'] = 'historic'
            df['BATCH_ID'] = self.batch_id
            df['LOADED_AT'] = self.batch_timestamp
            
            # Standard transformations
            for col in df.select_dtypes(include=['datetime64']).columns:
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            df = df.replace({pd.NA: None, pd.NaT: None})
            
            # Convert column names to uppercase for DuckDB
            df.columns = [col.upper() for col in df.columns]
            df = self.remove_duplicate_primary_keys(df, table_name)
            
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
            
            output_dir = Path('ingested_data')
            output_dir.mkdir(parents=True, exist_ok=True)
            
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

    def find_valid_date_column(self, df, table_name):
        """Find the first valid date column from the possible options"""
        possible_columns = self.get_date_column(table_name)
        
        for col in possible_columns:
            if col in df.columns:
                return col
                
        return None

    def get_date_column(self, table_name):
        """Return the appropriate date column for each table with fallbacks"""
        date_columns = {
            'customers': ['signup_date', 'last_login', 'created_at'],
            'orders': ['order_date', 'created_at', 'updated_at'],
            'products': ['created_at'],
            'order_items': ['created_at', 'order_date'],
            'reviews': ['created_at', 'order_date'],
            'interactions': ['event_date', 'created_at'],
            'categories': ['created_at'],
            'subcategories': ['created_at']
        }
        
        clean_table_name = table_name.replace('latest_', '')
        return date_columns.get(clean_table_name, ['created_at'])

    def run_initial_load(self):
        """Execute ETL process for initial historic data setup"""
        try:
            print("\nStarting initial historic data load...")
            
            # Create local directory for CSV files if it doesn't exist
            os.makedirs("ingested_data", exist_ok=True)
            
            for table in self.postgres_tables + self.s3_tables:
                print(f"\nProcessing {table}")
                
                try:
                    # Extract and combine data
                    historic_df = (self.extract_from_s3(table, 'historic') 
                                if table in self.s3_tables 
                                else self.extract_from_postgres(table, 'historic'))
                    
                    latest_df = (self.extract_from_s3(table, 'latest') 
                            if table in self.s3_tables 
                            else self.extract_from_postgres(table, 'latest'))
                    
                    # Find valid date column
                    date_column = self.find_valid_date_column(latest_df, table)
                    
                    if date_column:
                        min_latest_date = latest_df[date_column].min()
                        print(f"Latest data starts from: {min_latest_date}")
                    else:
                        print(f"Warning: No valid date column found for {table}")
                        min_latest_date = None
                    
                    # Combine and transform data
                    combined_df = pd.concat([historic_df, latest_df], ignore_index=True)
                    transformed_df = self.transform_data(combined_df, table)
                    
                    # Save to local CSV
                    combined_path = f"ingested_data/{table}_combined.csv"
                    transformed_df.to_csv(combined_path, index=False)
                    print(f"Saved combined data to {combined_path}")
                    
                    # Prepare metadata
                    metadata = {
                        'table_name': table,
                        'batch_id': self.batch_id,
                        'timestamp': self.batch_timestamp,
                        'historic_records': len(historic_df),
                        'latest_records': len(latest_df),
                        'total_records': len(combined_df),
                        'date_column_used': date_column,
                        'columns': transformed_df.columns.tolist(),
                        'data_types': transformed_df.dtypes.astype(str).to_dict(),
                        'min_date': min_latest_date if date_column else None
                    }
                    
                    # Save to S3 as historic data
                    self.save_to_s3_historic(transformed_df, table, metadata)
                    
                    
                    print(f"""
                    Completed processing for {table}:
                    - Historic records: {len(historic_df)}
                    - Latest records: {len(latest_df)}
                    - Total records processed: {len(combined_df)}
                    - Date column used: {date_column if date_column else 'None'}
                    - Data saved locally to: {combined_path}
                    - Data saved to S3 historic bucket
                    """)

                except Exception as e:
                    print(f"Error processing table {table}: {str(e)}")
                    continue

        except Exception as e:
            print(f"Initial load process error: {str(e)}")
            raise
        finally:
            print("\nInitial historic data load completed. Connections closed.")

if __name__ == "__main__":
    try:
        etl = InitialHistoricETL()
        etl.run_initial_load()
    except Exception as e:
        print(f"Main execution error: {str(e)}")
        raise