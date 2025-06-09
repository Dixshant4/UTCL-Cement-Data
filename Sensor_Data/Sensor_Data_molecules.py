import pandas as pd
import psycopg2
from datetime import datetime
import re

# Database connection details
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "sky@2025"
DB_HOST = "localhost"  # e.g., "localhost" or the PostgreSQL server IP
DB_PORT = "8089"

# Read the master table to create a Tag -> Tag_ID mapping
def get_tag_mapping(conn):
    query = 'SELECT Tag, Tag_ID FROM Master_Table;'
    df = pd.read_sql(query, conn)
    return dict(zip(df['tag'], df['tag_id']))

# Process an Excel file and insert data into sensor_data
def process_excel(file_path, conn):
    tag_mapping = get_tag_mapping(conn)

    # Read all sheets in the Excel file
    xls = pd.ExcelFile(file_path)
    # print(xls.sheet_names[1:])

    
    for sheet_name in xls.sheet_names:
        # Read the first row separately to get tag names
        df_header = pd.read_excel(xls, sheet_name, nrows=2, header=None)
        tag_names = df_header.iloc[1, 2:].tolist()  # Extract tag names from row 1 (ignoring first column)
        tag_names = [f'CLK_{tag}' for tag in tag_names]

        # Read the actual data starting from row 5
        df = pd.read_excel(xls, sheet_name, skiprows=1).iloc[:-1]  # Skip first 2 rows so row 3 becomes row 1 in df
        df.columns = ['Sample', 'Sampling Time'] + tag_names  # change column names based on KF or CLK sheet
        df = df[df['Sample'].str.upper() == 'CLINKER_Y1'] 
        # print(time_stamps_raw.head())  # Debugging

        time_stamps = pd.to_datetime(df['Sampling Time'], errors='coerce')  # Convert to datetime

        # print(time_stamps.head())  # Debugging
        # print(time_stamps.describe())  # Debugging

        tag_values = df[tag_names] 
        # print(tag_values.head())  # Debugging
        # print(tag_values.describe())  # Debugging
        # break

        # Assign correct tag names to columns
        # tag_values.columns = tag_names


        print(f"Sheet: {sheet_name}")
        print("Tag Names:", tag_names)  # Debugging
        # print(tag_values.head())  # Print first few rows to verify
        # Prepare data for insertion

        sensor_data = []
        time_inserted = datetime.now()

        for tag in tag_names:
            if tag in tag_mapping:  # Ensure the tag exists in Master_Table
                tag_id = tag_mapping[tag]
                for time, value in zip(time_stamps, tag_values[tag]):
                    try:
                        float_value = float(value)
                        sensor_data.append((time, tag_id, float_value, time_inserted))
                    except (ValueError, TypeError):
                        continue  # skip invalid values like '--'

        # Insert into sensor_data table
        insert_query = """
            INSERT INTO sensor_data_new (Time_stamp, Tag_Id, Tag_Value, time_inserted) 
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (Time_stamp, Tag_Id) 
            DO UPDATE SET Tag_Value = EXCLUDED.Tag_Value, time_inserted = EXCLUDED.time_inserted;
        """
        with conn.cursor() as cur:
            cur.executemany(insert_query, sensor_data)
            conn.commit()

def create_sensor_data_table(conn):
    """Create sensor_data table if it does not exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS sensor_data_new (
        Time_stamp TIMESTAMP NOT NULL,
        Tag_Id INT NOT NULL,
        Tag_Value FLOAT NOT NULL,
        time_inserted TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()
    print("✅ Table 'sensor_data' checked/created successfully.")

# Main function
def main():
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    try:
        create_sensor_data_table(conn)  # Ensure the table exists
        process_excel(r"D:\OneDrive - Aditya Birla Group\UTCL pre-processing data\raw_data\6-Month CLK.xls", conn)
    finally:
        conn.close()

if __name__ == "__main__":
    main()