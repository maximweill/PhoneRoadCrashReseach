import pandas as pd
import numpy as np
from pathlib import Path
import re

def get_sampling_rate_stats(time_values, is_ns=True):
    if len(time_values) < 2:
        return np.nan, np.nan, np.nan
    
    if is_ns:
        diffs = np.diff(time_values) * 1e-9  # convert to seconds
    else:
        diffs = np.diff(time_values)
        
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

def calculate_individual_characteristics(src_dir, output_path, meta_path=None):
    print(f"Processing {src_dir}...")

    if not src_dir.exists():
        print(f"Error: Directory {src_dir} does not exist.")
        return None

    files = list(src_dir.glob("*.csv"))
    if not files:
        print("No CSV files found in directory.")
        return None

    # Load metadata mapping if provided
    meta_map = {}
    if meta_path and meta_path.exists():
        meta_df = pd.read_csv(meta_path)
        # We assume the log has 'newfile' or 'file' columns from parse_raw_phone_data.py
        # 'newfile' is the cleaned version, 'file' is the original
        key_col = "newfile" if "newfile" in meta_df.columns else "file"
        
        meta_cols = ["Device", "Accelerometer", "Gyroscope", "Magnetometer"]
        available_meta = [c for c in meta_cols if c in meta_df.columns]
        
        for _, row in meta_df.iterrows():
            filename = row[key_col]
            meta_map[filename] = {c: row[c] for c in available_meta}

    results = []
    
    # Aligned with parse_raw_phone_data.py naming (Res instead of Mag)
    sensor_cols = [
        "LinAccX (m/s2)", "LinAccY (m/s2)", "LinAccZ (m/s2)", "LinAccRes (m/s2)",
        "RotVelX (rad/s)", "RotVelY (rad/s)", "RotVelZ (rad/s)", "RotVelRes (rad/s)",
        "RotAccX (rad/s2)", "RotAccY (rad/s2)", "RotAccZ (rad/s2)", "RotAccRes (rad/s2)",
        "magX_uT", "magY_uT", "magZ_uT", "magMag_uT"
    ]

    for file_path in files:
        try:
            row = {"filename": file_path.name}
            # Use comment='#' to handle raw files safely
            df = pd.read_csv(file_path, comment='#')
            
            # Merge metadata from log
            if file_path.name in meta_map:
                row.update(meta_map[file_path.name])

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
            if "Time (s)" in df.columns:
                mean_fs, median_fs, iqr_fs = get_sampling_rate_stats(df["Time (s)"].values, is_ns=False)
                row["fs_mean"] = mean_fs
                row["fs_median"] = median_fs
                row["fs_iqr"] = iqr_fs
            elif "time_ns" in df.columns:
                mean_fs, median_fs, iqr_fs = get_sampling_rate_stats(df["time_ns"].values, is_ns=True)
                row["fs_mean"] = mean_fs
                row["fs_median"] = median_fs
                row["fs_iqr"] = iqr_fs
            else:
                row["fs_mean"] = np.nan
                row["fs_median"] = np.nan
                row["fs_iqr"] = np.nan
                
            # Maximums (using absolute max for symmetry)
            for col in sensor_cols:
                if col in df.columns:
                    row[f"max_{col}"] = max(df[col].max(), -df[col].min())
                else:
                    row[f"max_{col}"] = np.nan
                    
            results.append(row)
            
        except Exception as e:
            print(f"  Error processing {file_path.name}: {e}")

    if results:
        df_results = pd.DataFrame(results)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df_results.to_csv(output_path, index=False)
        print(f"Exported individual characteristics to {output_path}")
        return df_results
    else:
        print("No results to export.")
        return None

def aggregate_characteristics(dfs=[], output_path="test_log_ignore/phone_characteristics_aggregated.csv"):
    dfs = [df for df in dfs if df is not None]
    if len(dfs) == 0:
        print("Error: no DataFrames provided for aggregation")
        return None
    
    df_individual = pd.concat(dfs, ignore_index=True)
    print("Aggregating characteristics by Phone ID...")
    
    df_individual["phone_id"] = df_individual["filename"].apply(extract_phone_id)
    
    agg_rules = {}
    avg_metrics = ["fs_mean", "fs_median", "fs_iqr", "mean_batt_temp_c", 
                   "initial_magX_uT", "initial_magY_uT", "initial_magZ_uT"]
    for col in avg_metrics:
        if col in df_individual.columns:
            agg_rules[col] = "mean"

    max_cols = [c for c in df_individual.columns if c.startswith("max_")]
    for col in max_cols:
        agg_rules[col] = "max"
        
    metadata_cols = ["Device", "Accelerometer", "Gyroscope", "Magnetometer"]
    for col in metadata_cols:
        if col in df_individual.columns:
            # Aggregate metadata using the most frequent non-null value
            agg_rules[col] = lambda x: x.dropna().mode().iloc[0] if not x.dropna().mode().empty else np.nan

    df_aggregated = df_individual.groupby("phone_id").agg(agg_rules).reset_index()

    # Metadata consistency check
    print("Checking for metadata inconsistencies...")
    for col in metadata_cols:
        if col in df_individual.columns:
            modes = df_aggregated.set_index("phone_id")[col]
            for _, row in df_individual.iterrows():
                val = row[col]
                phone_id = row["phone_id"]
                if pd.isna(val): continue
                    
                expected = modes[phone_id]
                if pd.notna(expected) and val != expected:
                     print(f"  WARNING: Inconsistent {col} for {row['filename']}: expected '{expected}', found '{val}'")
    
    numeric_cols = df_aggregated.select_dtypes(include=[np.number]).columns
    df_aggregated[numeric_cols] = df_aggregated[numeric_cols].round(3)
    
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_aggregated.to_csv(output_path, index=False)
    print(f"Exported aggregated characteristics to {output_path}")
    
    return df_aggregated

if __name__ == "__main__":
    characteristics_dfs = [
        calculate_individual_characteristics(
            src_dir=Path("phone_drop_test_data_ignore/phone_cleaned"),
            output_path=Path("test_log_ignore/phone_characteristics_drops.csv"),
            meta_path=Path("test_log_ignore/deduplication_log2.csv")
        ),
        calculate_individual_characteristics(
            src_dir=Path("stationary_ignore/cleaned"),
            output_path=Path("test_log_ignore/phone_characteristics_stationary.csv"),
            meta_path=Path("test_log_ignore/deduplication_stationary.csv")
        )
    ]
    aggregate_characteristics(characteristics_dfs)
