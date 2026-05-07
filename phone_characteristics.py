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

def extract_metadata(file_path):
    """
    Extracts metadata from the first 4 lines of a CSV file if they start with #.
    """
    metadata = {}
    try:
        with open(file_path, 'r') as f:
            for _ in range(4):
                line = f.readline()
                if line.startswith("#"):
                    parts = line.lstrip("#").split(":", 1)
                    if len(parts) == 2:
                        key = parts[0].strip()
                        value = parts[1].strip()
                        metadata[key] = value
    except Exception as e:
        print(f"  Error extracting metadata from {file_path.name}: {e}")
    return metadata

def calculate_individual_characteristics():
    cleaned_dir = Path("phone_drop_test_data_ignore/phone_cleaned")
    source_dir_with_header = Path("phone_drop_test_data_ignore/phone_01052026")
    output_path = Path("test_log_ignore/phone_characteristics.csv")
    
    if not cleaned_dir.exists():
        print(f"Error: Directory {cleaned_dir} does not exist.")
        return None

    files = list(cleaned_dir.glob("*.csv")) + list(cleaned_dir.glob("*.parquet"))
    if not files:
        print("No CSV or Parquet files found in cleaned directory.")
        return None

    results = []
    
    sensor_cols = [
        "LinAccX (m/s2)", "LinAccY (m/s2)", "LinAccZ (m/s2)", "LinAccMag (m/s2)",
        "RotVelX (rad/s)", "RotVelY (rad/s)", "RotVelZ (rad/s)", "RotMag (rad/s)",
        "RotAccX (rad/s2)", "RotAccY (rad/s2)", "RotAccZ (rad/s2)", "RotAccMag (rad/s2)",
        "magX_uT", "magY_uT", "magZ_uT", "magMag_uT"
    ]

    for file_path in files:
        print(f"Processing {file_path.name}...")
        try:
            if file_path.suffix == ".csv":
                df = pd.read_csv(file_path)
            elif file_path.suffix == ".parquet":
                df = pd.read_parquet(file_path)
            else:
                continue
            
            # Basic stats
            row = {"filename": file_path.name}
            
            # Metadata from source header if available
            source_file = source_dir_with_header / file_path.name
            if source_file.exists():
                metadata = extract_metadata(source_file)
                for key, value in metadata.items():
                    row[key] = value
            
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
        
    # Metadata fields to aggregate with mode
    metadata_cols = ["Device", "Accelerometer", "Gyroscope", "Magnetometer"]
    for col in metadata_cols:
        if col in df_individual.columns:
            agg_rules[col] = lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan

    df_aggregated = df_individual.groupby("phone_id").agg(agg_rules).reset_index()

    # Check for metadata inconsistencies
    print("Checking for metadata inconsistencies...")
    for col in metadata_cols:
        if col in df_individual.columns:
            # Map phone_id to its mode for this column
            modes = df_aggregated.set_index("phone_id")[col]
            for _, row in df_individual.iterrows():
                val = row[col]
                phone_id = row["phone_id"]
                expected = modes[phone_id]
                
                # Check if both values exist and differ
                if pd.notna(expected) and pd.notna(val) and val != expected:
                     print(f"  WARNING: Inconsistent {col} for {row['filename']}: expected '{expected}', found '{val}'")
    
    # Rounding for cleanliness (only for numeric columns)
    numeric_cols = df_aggregated.select_dtypes(include=[np.number]).columns
    df_aggregated[numeric_cols] = df_aggregated[numeric_cols].round(3)
    
    df_aggregated.to_csv(output_path, index=False)
    print(f"Exported aggregated characteristics to {output_path}")

if __name__ == "__main__":
    characteristics_df = calculate_individual_characteristics()
    aggregate_characteristics(characteristics_df)
