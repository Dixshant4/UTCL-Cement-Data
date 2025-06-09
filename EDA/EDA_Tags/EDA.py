import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import os

# Database connection details
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "sky@2025"
DB_HOST = "localhost"
DB_PORT = "8089"

def connect_db():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )

def get_tag_metadata(conn):
    """Fetch all Tag_ID to Description_2 mappings from Master_Table."""
    query = "SELECT Tag_ID, Description_2, unit  FROM Master_Table;"
    df = pd.read_sql(query, conn)
    return dict(zip(df['tag_id'], zip(df['description_2'], df['unit'])))

def get_unique_tag_ids(conn):
    query = "SELECT DISTINCT tag_id FROM sensor_data_new;"
    # Ensure we get a list of tag ids (converted to int if necessary)
    return pd.read_sql(query, conn)['tag_id'].tolist()

def perform_eda_for_tag(conn, tag_id, description, unit, save_plots=True):
    # Query both the timestamp and sensor value, ordered by time_stamp
    query = f"""
    SELECT Time_stamp, Tag_Value FROM sensor_data_new
    WHERE Tag_Id = {tag_id}
    ORDER BY Time_stamp;
    """
    df = pd.read_sql(query, conn)
    # Convert columns to lower-case for consistency
    df.columns = df.columns.str.lower()
    
    # --- Basic Statistics for Tag_Value ---
    count = df['tag_value'].count()
    missing = df['tag_value'].isna().sum()
    min_val = df['tag_value'].min()
    max_val = df['tag_value'].max()
    mean_val = df['tag_value'].mean()
    std_val = df['tag_value'].std()
    q25 = df['tag_value'].quantile(0.25)
    median_val = df['tag_value'].median()
    q75 = df['tag_value'].quantile(0.75)
    
    # --- Time Range Coverage ---
    time_min = df['time_stamp'].min()
    time_max = df['time_stamp'].max()
    
    # --- Sampling Frequency and Gap Analysis ---
    # Ensure data is sorted by timestamp
    df_sorted = df.sort_values("time_stamp")
    # Calculate time differences between consecutive records
    time_diff = df_sorted['time_stamp'].diff().dropna()
    # Convert the time differences to seconds for easier interpretation
    time_diff_seconds = time_diff.dt.total_seconds()
    
    # Compute the median sampling time difference and count gaps greater than twice that median
    median_diff = time_diff_seconds.median()
    gap_threshold = 2 * median_diff
    num_gaps = (time_diff_seconds > gap_threshold).sum()
    
    # --- Prepare EDA Summary ---
    tag_stats = {
        "Tag_Id": tag_id,
        "Description_2": description,
        'unit': unit,
        "Count": count,
        "Missing Values": missing,
        "Min": min_val,
        "Max": max_val,
        "Mean": mean_val,
        "Std Dev": std_val,
        "25%": q25,
        "Median": median_val,
        "75%": q75,
        "Time Range Start": time_min,
        "Time Range End": time_max,
        "Median Sampling Diff (sec)": median_diff,
        "Gaps (>2x median diff)": num_gaps,
    }
    
    print(f"\n📊 EDA for Tag_Id {tag_id} ({description})")
    print("-" * 40)
    for k, v in tag_stats.items():
        print(f"{k}: {v}")
    
    # Get the directory of the currently running script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Define subdirectories relative to the script's location
    hist_dir = os.path.join(script_dir, "histograms")
    box_dir = os.path.join(script_dir, "boxplots")



    # --- Visualization ---
    if save_plots:
        # Create those directories if they don't exist
        os.makedirs(hist_dir, exist_ok=True)
        os.makedirs(box_dir, exist_ok=True)
        
        # Histogram for Tag_Value
        plt.figure(figsize=(8, 5))
        # df[df['tag_value'].between(0, 3000)]['tag_value'].hist(bins=50)
        df['tag_value'].hist(bins=50)
        plt.title(f"Histogram for Tag_Id {tag_id} ({description})")
        plt.xlabel(f"Tag Value ({unit})")
        plt.ylabel("Frequency")
        plt.savefig(os.path.join(hist_dir, f"Tag_{tag_id}_histogram.png"))
        # plt.savefig(f"histograms/Tag_{tag_id}_histogram.png")
        plt.close()
        
        # Boxplot for Tag_Value
        plt.figure(figsize=(6, 8))
        plt.boxplot(df['tag_value'].dropna(), vert=True)
        plt.title(f"Boxplot for Tag_Id {tag_id} ({description})")
        plt.ylabel(f"Tag Value ({unit})")
        plt.savefig(os.path.join(box_dir, f"Tag_{tag_id}_boxplot.png"))
        # plt.savefig(f"boxplots/Tag_{tag_id}_boxplot.png")
        plt.close()
    
    return tag_stats

def main():
    conn = connect_db()
    try:
        tag_ids = get_unique_tag_ids(conn)
        tag_meta = get_tag_metadata(conn)

        summary_list = []
        for tag_id in tag_ids:
            description, unit = tag_meta.get(tag_id, ("Unknown", ""))
            stats = perform_eda_for_tag(conn, tag_id, description, unit)
            summary_list.append(stats)
        
        # Save the overall EDA summary for all tags
        summary_df = pd.DataFrame(summary_list)
        summary_df.to_csv("tag_eda_summary.csv", index=False, encoding='utf-8-sig')
        print("\n✅ EDA complete. Summary saved to 'tag_eda_summary.csv'.")
    
    finally:
        conn.close()

if __name__ == "__main__":
    main()
