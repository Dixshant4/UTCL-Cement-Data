import pandas as pd
import psycopg2

"""This code creates the tags in master_table from the pysro variables"""

# Database connection details
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "sky@2025"
DB_HOST = "localhost"  # e.g., "localhost" or the PostgreSQL server IP
DB_PORT = "8089"

# Path to your Excel file
EXCEL_FILE = r"D:\OneDrive - Aditya Birla Group\UTCL pre-processing data\raw_data\Pysro1.xlsx"

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
)
cursor = conn.cursor()

# Create table if not exists
cursor.execute("""
    CREATE TABLE IF NOT EXISTS master_table1 (
        tag_id SERIAL PRIMARY KEY,
        tag TEXT UNIQUE,
        unit TEXT,
        max INT,
        min INT,
        description TEXT
    );
""")
conn.commit()

# Load the Excel file
xls = pd.ExcelFile(EXCEL_FILE)

# Process each sheet
for sheet_name in xls.sheet_names:
    df = pd.read_excel(xls, sheet_name=sheet_name, header=None)  # Read sheet
    df = df.iloc[:5]  # Take only the first 5 rows

    # Transpose DataFrame so the first column becomes headers
    df = df.set_index(0).T  
    print(sheet_name)
    df.columns = df.columns.str.strip()  # Removes leading/trailing spaces from column names
    print("Columns after transposing:", df.columns.tolist())

    # Rename columns to match expected database schema
    df.rename(columns={"Time": "Tag", "Y-max": "Max", "Y-min": "Min", "Unit": "Unit", "Description": "Description"}, inplace=True)
    print("Columns after transposing:", df.columns.tolist())

    # Insert data into PostgreSQL
    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO master_table1 (tag, max, min, unit, description)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (tag) DO NOTHING;
            """, (row["Tag"], row["Max"], row["Min"], row["Unit"], row["Description"]))
        except Exception as e:
            print(f"Error inserting row {row['Tag']}: {e}")

    conn.commit()

# Close connection
cursor.close()
conn.close()
print("Data import complete!")
