import pandas as pd
import numpy as np
from pathlib import Path
import re

def get_sampling_rate_stats(time_ns):
    if len(time_ns) < 2:
        return np.nan, np.nan, np.nan
    
    diffs = np.diff(time_ns) * 1e-9  # convert to seconds
    # Filter out zero or negative diffs to avoid division by zero
    diffs = diffs[diffs > 0]
    
    if len(diffs) == 0:
        return np.nan, np.nan, np.nan
        
    freqs = 1.0 / diffs
    
    mean_freq = np.mean(freqs)
    median_freq = np.median(freqs)
    q75, q25 = np.percentile(freqs, [75, 25])
    iqr_freq = q75 - q25
    
    return mean_freq, median_freq, iqr_freq

def extract_phone_id(filename):
    """
    Extracts PhoneID from filename (e.g., Phone001, Phone_001).
    """
    match = re.search(r"Phone_?(\d+)", filename, re.IGNORECASE)
    if match:
        return f"Phone{match.group(1)}"
    return "Unknown"

def calculate_individual_characteristics():
    cleaned_dir = Path("phone_drop_test_data_ignore/phone_cleaned")
    output_path = Path("test_log_ignore/phone_characteristics.csv")
    
    if not cleaned_dir.exists():
        print(f"Error: Directory {cleaned_dir} does not exist.")
        return None

    csv_files = list(cleaned_dir.glob("*.csv"))
    if not csv_files:
        print("No CSV files found in cleaned directory.")
        return None

    results = []
    
    sensor_cols = [
        "accelX_g", "accelY_g", "accelZ_g", "accelMag_g",
        "gyroX_dps", "gyroY_dps", "gyroZ_dps", "gyroMag_dps",
        "magX_uT", "magY_uT", "magZ_uT", "magMag_uT"
    ]

    for file_path in csv_files:
        print(f"Processing {file_path.name}...")
        try:
            df = pd.read_csv(file_path)
            
            # Basic stats
            row = {"filename": file_path.name}
            
            # Battery temperature
            if "batt_temp_c" in df.columns:
                row["mean_batt_temp_c"] = df["batt_temp_c"].mean()
            else:
                row["mean_batt_temp_c"] = np.nan

            # Initial magnetic field (mean of first 100 samples)
            for axis in ["X", "Y", "Z"]:
                col = f"mag{axis}_uT"
                if col in df.columns:
                    row[f"initial_{col}"] = df[col].iloc[:100].mean()
                else:
                    row[f"initial_{col}"] = np.nan

            # Sampling rate stats
            if "time_ns" in df.columns:
                mean_fs, median_fs, iqr_fs = get_sampling_rate_stats(df["time_ns"].values)
                row["fs_mean"] = mean_fs
                row["fs_median"] = median_fs
                row["fs_iqr"] = iqr_fs
            else:
                row["fs_mean"] = np.nan
                row["fs_median"] = np.nan
                row["fs_iqr"] = np.nan
                
            # Maximums
            for col in sensor_cols:
                if col in df.columns:
                    row[f"max_{col}"] = max(df[col].max(),-df[col].min())
                else:
                    row[f"max_{col}"] = np.nan
                    
            results.append(row)
            
        except Exception as e:
            print(f"  Error processing {file_path.name}: {e}")

    if results:
        df_results = pd.DataFrame(results)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df_results.to_csv(output_path, index=False)
        print(f"\nExported individual characteristics to {output_path}")
        return df_results
    else:
        print("No results to export.")
        return None

def aggregate_characteristics(df_individual=None):
    input_path = Path("test_log_ignore/phone_characteristics.csv")
    output_path = Path("test_log_ignore/phone_characteristics_aggregated.csv")
    
    if df_individual is None:
        if not input_path.exists():
            print(f"Error: {input_path} not found for aggregation.")
            return
        df_individual = pd.read_csv(input_path)

    print("Aggregating characteristics by Phone ID...")
    
    # Extract Phone ID
    df_individual["phone_id"] = df_individual["filename"].apply(extract_phone_id)
    
    # Define aggregation rules
    # For sampling rates, we take the mean across all records for that phone
    # For maximums, we take the absolute maximum across all records
    agg_rules = {
        "fs_mean": "mean",
        "fs_median": "mean",
        "fs_iqr": "mean"
    }
    
    # Add new metrics to aggregation rules
    new_metrics = ["mean_batt_temp_c", "initial_magX_uT", "initial_magY_uT", "initial_magZ_uT"]
    for col in new_metrics:
        if col in df_individual.columns:
            agg_rules[col] = "mean"

    max_cols = [c for c in df_individual.columns if c.startswith("max_")]
    for col in max_cols:
        agg_rules[col] = "max"
        
    df_aggregated = df_individual.groupby("phone_id").agg(agg_rules).reset_index()
    
    # Rounding for cleanliness
    df_aggregated = df_aggregated.round(3)
    
    df_aggregated.to_csv(output_path, index=False)
    print(f"Exported aggregated characteristics to {output_path}")

if __name__ == "__main__":
    df = calculate_individual_characteristics()
    aggregate_characteristics(df)
