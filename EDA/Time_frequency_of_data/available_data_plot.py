# import pandas as pd
# import psycopg2
# import matplotlib.pyplot as plt
# from matplotlib.backends.backend_pdf import PdfPages
# import os

# # --- DB Connection ---
# DB_NAME = "postgres"
# DB_USER = "postgres"
# DB_PASSWORD = "sky@2025"
# DB_HOST = "localhost"
# DB_PORT = "8089"

# # --- Fetch time-tag pairs ---
# def fetch_time_series(conn):
#     query = """
#         SELECT time_stamp, tag_id 
#         FROM sensor_data_new;
#     """
#     df = pd.read_sql(query, conn)
#     df['time_stamp'] = pd.to_datetime(df['time_stamp'])
#     return df

# # --- Plotting function ---
# def plot_and_save_images(df, output_dir="eda_plots"):
#     os.makedirs(output_dir, exist_ok=True)
    
#     tag_ids = sorted(df['tag_id'].unique())

#     tag_id_map = {tag: i for i, tag in enumerate(tag_ids)}  # Compress tag IDs
#     df['tag_index'] = df['tag_id'].map(tag_id_map)

#     chunk_size = 10

#     for i in range(0, len(tag_ids), chunk_size):
#         tag_chunk = tag_ids[i:i + chunk_size]
#         chunk_indexes = [tag_id_map[tag] for tag in tag_chunk]
#         chunk_df = df[df['tag_id'].isin(tag_chunk)]

#         plt.figure(figsize=(18, 8))
#         plt.scatter(chunk_df['time_stamp'], chunk_df['tag_index'], s=0.5, alpha=0.6)

#         plt.yticks(chunk_indexes, tag_chunk)  # Set original tag_ids as labels)

#         plt.xlabel("Timestamp (minute-level)", fontsize=12)
#         plt.ylabel("Tag ID", fontsize=12)
#         plt.title(f"Sensor Data Coverage (Tags {tag_chunk[0]}–{tag_chunk[-1]})", fontsize=14)
#         plt.grid(True)
#         plt.tight_layout()

#         filename = f"{output_dir}/tag_plot1_{i//chunk_size + 1}.png"
#         plt.savefig(filename, dpi=300)
#         plt.close()
#         print(f"✅ Saved: {filename}")

# # --- Main ---
# def main():
#     conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
#     try:
#         df = fetch_time_series(conn)
#         plot_and_save_images(df)
#     finally:
#         conn.close()

# if __name__ == "__main__":
#     main()

import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import os

# --- DB Connection ---
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "sky@2025"
DB_HOST = "localhost"
DB_PORT = "8089"

# --- Fetch sensor data ---
def fetch_sensor_data(conn):
    query = """
        SELECT time_stamp, tag_id 
        FROM sensor_data_new;
    """
    df = pd.read_sql(query, conn)
    df['time_stamp'] = pd.to_datetime(df['time_stamp'])
    return df

# --- Fetch tag descriptions ---
def fetch_tag_descriptions(conn):
    query = """
        SELECT tag_id, description 
        FROM master_table;
    """
    return pd.read_sql(query, conn)

# --- Plotting function ---
def plot_and_save_images(df, output_dir="eda_plots"):
    os.makedirs(output_dir, exist_ok=True)

    desc_list = sorted(df['description'].unique())
    desc_map = {desc: i for i, desc in enumerate(desc_list)}
    df['desc_index'] = df['description'].map(desc_map)

    chunk_size = 10
    for i in range(0, len(desc_list), chunk_size):
        chunk_descs = desc_list[i:i + chunk_size]
        chunk_indexes = [desc_map[d] for d in chunk_descs]
        chunk_df = df[df['description'].isin(chunk_descs)]

        plt.figure(figsize=(18, 8))
        plt.scatter(chunk_df['time_stamp'], chunk_df['desc_index'], s=0.5, alpha=0.6)
        plt.yticks(chunk_indexes, chunk_descs)
        plt.xlabel("Timestamp (minute-level)", fontsize=12)
        plt.ylabel("Sensor Description", fontsize=12)
        plt.title(f"Sensor Data Coverage ({chunk_descs[0]}–{chunk_descs[-1]})", fontsize=14)
        plt.grid(True)
        plt.tight_layout()

        filename = f"{output_dir}/desc_plot_{i//chunk_size + 1}.png"
        plt.savefig(filename, dpi=300)
        plt.close()
        print(f"✅ Saved: {filename}")

# --- Main ---
def main():
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    try:
        sensor_df = fetch_sensor_data(conn)
        desc_df = fetch_tag_descriptions(conn)

        merged_df = pd.merge(sensor_df, desc_df, on='tag_id', how='left')
        plot_and_save_images(merged_df)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
