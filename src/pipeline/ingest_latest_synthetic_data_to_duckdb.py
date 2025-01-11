import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from faker import Faker
import duckdb
from pathlib import Path
from typing import Dict, Any

class EcommerceDataLoader:
    def __init__(self, db_path: str = "ecommerce.duckdb"):
        """Initialize DuckDB connection"""
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._init_database()

    def _init_database(self):
        """Initialize database schema"""
        # Create schemas if they don't exist
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
        
        # Create all required tables with appropriate data types
        tables_schema = {
            'customers': """
                CREATE TABLE IF NOT EXISTS raw.customers (
                    customer_id BIGINT PRIMARY KEY,
                    email VARCHAR,
                    first_name VARCHAR,
                    last_name VARCHAR,
                    age INTEGER,
                    gender VARCHAR,
                    annual_income DOUBLE,
                    marital_status VARCHAR,
                    education VARCHAR,
                    location_type VARCHAR,
                    city VARCHAR,
                    state VARCHAR,
                    country VARCHAR,
                    signup_date TIMESTAMP,
                    last_login TIMESTAMP,
                    preferred_channel VARCHAR,
                    is_active BOOLEAN,
                    created_at TIMESTAMP
                )
            """,
            'products': """
                CREATE TABLE IF NOT EXISTS raw.products (
                    product_id BIGINT PRIMARY KEY,
                    category_id BIGINT,
                    subcategory_id BIGINT,
                    product_name VARCHAR,
                    description VARCHAR,
                    base_price DOUBLE,
                    sale_price DOUBLE,
                    stock_quantity INTEGER,
                    weight_kg DOUBLE,
                    is_active BOOLEAN,
                    created_at TIMESTAMP,
                    brand VARCHAR,
                    sku VARCHAR,
                    rating DOUBLE,
                    review_count INTEGER
                )
            """,
            'categories': """
                CREATE TABLE IF NOT EXISTS raw.categories (
                    category_id BIGINT PRIMARY KEY,
                    category_name VARCHAR,
                    created_at TIMESTAMP
                )
            """,
            'subcategories': """
                CREATE TABLE IF NOT EXISTS raw.subcategories (
                    subcategory_id BIGINT PRIMARY KEY,
                    category_id BIGINT,
                    subcategory_name VARCHAR,
                    created_at TIMESTAMP
                )
            """,
            'orders': """
                CREATE TABLE IF NOT EXISTS raw.orders (
                    order_id BIGINT PRIMARY KEY,
                    customer_id BIGINT,
                    order_date TIMESTAMP,
                    status VARCHAR,
                    total_amount DOUBLE,
                    shipping_cost DOUBLE,
                    payment_method VARCHAR,
                    shipping_address VARCHAR,
                    billing_address VARCHAR,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
            """,
            'order_items': """
                CREATE TABLE IF NOT EXISTS raw.order_items (
                    order_item_id BIGINT PRIMARY KEY,
                    order_id BIGINT,
                    product_id BIGINT,
                    quantity INTEGER,
                    unit_price DOUBLE,
                    total_price DOUBLE,
                    created_at TIMESTAMP
                )
            """,
            'reviews': """
                CREATE TABLE IF NOT EXISTS raw.reviews (
                    review_id BIGINT PRIMARY KEY,
                    product_id BIGINT,
                    order_id BIGINT,
                    customer_id BIGINT,
                    review_score INTEGER,
                    review_text VARCHAR,
                    created_at TIMESTAMP
                )
            """,
            'interactions': """
                CREATE TABLE IF NOT EXISTS raw.interactions (
                    event_id BIGINT PRIMARY KEY,
                    customer_id BIGINT,
                    product_id BIGINT,
                    event_type VARCHAR,
                    event_date TIMESTAMP,
                    device_type VARCHAR,
                    session_id VARCHAR,
                    created_at TIMESTAMP
                )
            """
        }
        
        # Create all tables
        for table_sql in tables_schema.values():
            self.conn.execute(table_sql)

    def load_data(self, data_dict: Dict[str, pd.DataFrame]):
        """Load all dataframes into DuckDB tables"""
        try:
            # Start transaction
            self.conn.begin()
            
            for table_name, df in data_dict.items():
                print(f"Loading {table_name}...")
                
                # Convert datetime columns to proper format
                datetime_cols = df.select_dtypes(include=['datetime64']).columns
                for col in datetime_cols:
                    df[col] = pd.to_datetime(df[col])
                
                # Load data into table
                self.conn.execute(f"DELETE FROM raw.{table_name}")
                self.conn.register(f"temp_{table_name}", df)
                self.conn.execute(f"INSERT INTO raw.{table_name} SELECT * FROM temp_{table_name}")
                
                # Get row count
                count = self.conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()[0]
                print(f"Loaded {count} rows into {table_name}")
            
            # Commit transaction
            self.conn.commit()
            print("\nAll data loaded successfully!")
            
        except Exception as e:
            # Rollback on error
            self.conn.rollback()
            print(f"Error loading data: {str(e)}")
            raise
        
    def export_data(self, output_dir: str = "duckdb_exports"):
        """Export all tables to CSV files"""
        try:
            # Create output directory
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            
            # Get all tables
            tables = self.conn.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'raw'
            """).fetchall()
            
            # Export each table
            for (table_name,) in tables:
                output_file = output_path / f"{table_name}.csv"
                self.conn.execute(f"""
                    COPY (SELECT * FROM raw.{table_name}) 
                    TO '{output_file}' (HEADER, DELIMITER ',')
                """)
                print(f"Exported {table_name} to {output_file}")
                
        except Exception as e:
            print(f"Error exporting data: {str(e)}")
            raise
    
    def close(self):
        """Close the database connection"""
        self.conn.close()

def save_data(data_dict: Dict[str, pd.DataFrame], db_path: str = "ecommerce.duckdb"):
    """Save all generated data to DuckDB"""
    try:
        # Initialize loader
        loader = EcommerceDataLoader(db_path)
        
        # Load all data
        loader.load_data(data_dict)
        
        # Export data to CSV files
        loader.export_data()
        
        # Close connection
        loader.close()
        
        print("\nData Generation and Loading Summary:")
        for table_name, df in data_dict.items():
            print(f"\n{table_name.upper()} Table:")
            print(f"Total records: {len(df)}")
            
            # Different tables have different date columns
            date_columns = {
                'customers': 'signup_date',
                'products': 'created_at',
                'orders': 'order_date',
                'order_items': 'created_at',
                'reviews': 'created_at',
                'interactions': 'event_date',
                'categories': 'created_at',
                'subcategories': 'created_at'
            }
            
            # Get the appropriate date column for this table
            date_col = date_columns.get(table_name)
            if date_col and date_col in df.columns:
                print(f"Date range: {df[date_col].min()} to {df[date_col].max()}")
    
    except Exception as e:
        print(f"Error saving data: {str(e)}")
        raise

# Example usage
if __name__ == "__main__":
    from generate_data import RecentEcommerceDataGenerator
    
    # Generate data
    generator = RecentEcommerceDataGenerator()
    data = generator.generate_all_data()
    
    # Save data to DuckDB
    save_data(data)