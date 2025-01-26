import pandas as pd
import mysql.connector
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_import.log"),
        logging.StreamHandler()
    ]
)

# List of CSV files and their corresponding table names
csv_files = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('sellers.csv', 'sellers'),
    ('products.csv', 'products'),
    ('payments.csv', 'payments'),
    ('order_items.csv', 'order_items'),
    ('geolocation.csv', 'geolocation')  # Include geolocation.csv
]

# Connect to the MySQL database
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='12345',
    database='ecomdb'
)
cursor = conn.cursor()

# Folder containing the CSV files
folder_path = 'C:/Users/LENOVO/Documents/Abdul/Data Analytics/Ecommerce'

def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

# Chunk size for data insertion
chunk_size = 1000

for csv_file, table_name in csv_files:
    try:
        file_path = os.path.join(folder_path, csv_file)
        logging.info(f"Processing file: {file_path}")
        
        # Check if the file exists
        if not os.path.exists(file_path):
            logging.warning(f"File not found: {file_path}. Skipping...")
            continue
        
        # Read the CSV file in chunks
        for chunk in pd.read_csv(file_path, encoding='utf-8', chunksize=chunk_size):
            # Replace NaN with None to handle SQL NULL
            chunk = chunk.where(pd.notnull(chunk), None)
            
            # Clean column names
            chunk.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in chunk.columns]
            
            # Check if the table exists
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            if not cursor.fetchone():
                # Generate the CREATE TABLE statement for the first chunk
                columns = ', '.join([f'`{col}` {get_sql_type(chunk[col].dtype)}' for col in chunk.columns])
                create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
                cursor.execute(create_table_query)
            
            # Use executemany for bulk insert
            sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in chunk.columns])}) VALUES ({', '.join(['%s'] * len(chunk.columns))})"
            values = [tuple(None if pd.isna(x) else x for x in row) for row in chunk.to_numpy()]
            
            cursor.executemany(sql, values)
            
            # Commit the transaction
            conn.commit()
            logging.info(f"Processed a chunk of {csv_file} into table `{table_name}`.")
    
    except Exception as e:
        logging.error(f"Error processing {csv_file}: {e}")
        conn.rollback()

# Close the connection
conn.close()

