import pandas as pd
import psycopg2

"""This code creates the raw materials tags in master_table """

# Database connection details
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "sky@2025"
DB_HOST = "localhost"  # e.g., "localhost" or the PostgreSQL server IP
DB_PORT = "8089"

# Path to your Excel file
EXCEL_FILE = r"D:\OneDrive - Aditya Birla Group\UTCL pre-processing data\raw_data\KF- 6 MONTH.xls"

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
)
cursor = conn.cursor()

# Create table if not exists
cursor.execute("""
    CREATE TABLE IF NOT EXISTS master_table (
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
    df = df.iloc[:2]

    # Transpose DataFrame so the first column becomes headers
    df = df.set_index(0).T      
    df = pd.concat([df.iloc[:0], df.iloc[1:]], ignore_index=True) # Keep the first row as headers and drop the second row and continue from 3rd row (removes sample row)
    
    print(sheet_name)
    df.columns = df.columns.str.strip()  # Removes leading/trailing spaces from column names
    print("Columns after transposing:", df.columns.tolist())

    # Rename columns to match expected database schema
    df.rename(columns={"Date": "Tag"}, inplace=True) # change sample to date for KF sheet
    print("Columns after transposing:", df.columns.tolist())

    # Insert data into PostgreSQL
    for _, row in df.iterrows():
        tag_value = str(row.get("Tag", "")).strip()  # Ensure tag is a string and strip whitespace
        if not tag_value:
            continue  # skip empty tags

        formatted_tag = f"KF_{tag_value}"  # add 'CLK_' prefix
        try:
            cursor.execute("""
                INSERT INTO master_table (tag, description)
                VALUES (%s, %s)
                ON CONFLICT (tag) DO NOTHING;
            """, (formatted_tag, formatted_tag,))
            print(formatted_tag)
        except Exception as e:
            print(f"Error inserting row {formatted_tag}: {e}")

    conn.commit()

# Close connection
cursor.close()
conn.close()
print("Data import complete!")
