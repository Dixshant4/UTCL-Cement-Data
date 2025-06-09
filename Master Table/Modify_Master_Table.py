import pandas as pd
import psycopg2

"""this code helps add description_2 and site columns to master_table"""

# Database connection details
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "sky@2025"
DB_HOST = "localhost"
DB_PORT = "8089"

# Path to the second Excel file
EXCEL_FILE_2 = r"D:\OneDrive - Aditya Birla Group\UTCL pre-processing data\comparision of variables.xlsx"

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
)
cursor = conn.cursor()

# Add new columns to master_table if they don’t exist
cursor.execute("""
    ALTER TABLE master_table
    ADD COLUMN IF NOT EXISTS site TEXT,
    ADD COLUMN IF NOT EXISTS description_2 TEXT;
""")
conn.commit()

# Load the second Excel file
df_mapping = pd.read_excel(EXCEL_FILE_2)

# Ensure Tags column is of type string and drop NaN values
df_mapping["Tags"] = df_mapping["Tags"].astype(str).str.strip()
df_mapping = df_mapping[df_mapping["Tags"].notna() & (df_mapping["Tags"] != "nan")]

# Iterate over rows to update database
for _, row in df_mapping.iterrows():
    tag = row["Tags"]
    site = row["Site"]
    description_2 = row["Description 2"]
    

    # Update only if tag is valid
    if tag and tag.lower() != "nan":
        # print(description_2)
        cursor.execute("""
            UPDATE master_table
            SET site = %s, description_2 = %s
            WHERE tag = %s;
        """, (site, description_2, tag))

conn.commit()

# Close connection
cursor.close()
conn.close()

print("Database update complete!")
