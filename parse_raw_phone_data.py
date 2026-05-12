import pandas as pd
import numpy as np
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import re

# =========================================================
# CONSTANTS & HELPERS
# =========================================================

G = 9.80665
DEG2RAD = np.pi / 180.0

def extract_metadata(file_path):
    """Dynamically extracts metadata from file headers starting with '#'."""
    metadata = {}
    try:
        with open(file_path, "r") as f:
            for line in f:
                if not line.startswith("#"):
                    break
                parts = line.lstrip("#").split(":", 1)
                if len(parts) == 2:
                    metadata[parts[0].strip()] = parts[1].strip()
    except Exception:
        pass
    return metadata

def parse_test_name(test_name):
    """Extracts speed and repeat number from test name (e.g., '2ms V1')."""
    if not isinstance(test_name, str):
        return None, None
    match = re.search(r"(\d+)ms V(\d+)", test_name)
    return (match.group(1), match.group(2)) if match else (None, None)

def extract_phone_id(filename):
    """Extracts phone ID from filename (e.g., 'Phone001')."""
    match = re.search(r"Phone_?(\d+)", filename, re.IGNORECASE)
    return f"Phone{match.group(1)}" if match else "Unknown"

# =========================================================
# UNIT CONVERSION
# =========================================================

def change_units(df):
    """Converts raw sensor units to SI, calculates resultants, and preserves auxiliary columns."""
    # Times in float64 for precision
    t = df["time_ns"].to_numpy(dtype=np.float64) / 1e9
    
    # Sensor data in float32 for efficiency
    accel = df[["accelX_g", "accelY_g", "accelZ_g"]].to_numpy(dtype=np.float32) * G
    gyro = df[["gyroX_dps", "gyroY_dps", "gyroZ_dps"]].to_numpy(dtype=np.float32) * DEG2RAD
    
    # Gradient for rotational acceleration
    rot_acc = np.gradient(gyro, t, axis=0)
    
    # Resultants
    lin_acc_res = np.linalg.norm(accel, axis=1)
    rot_vel_res = np.linalg.norm(gyro, axis=1)
    rot_acc_res = np.linalg.norm(rot_acc, axis=1)

    res_df = pd.DataFrame({
        "Time (s)": t,
        "LinAccX (m/s2)": accel[:, 0], "LinAccY (m/s2)": accel[:, 1], "LinAccZ (m/s2)": accel[:, 2],
        "RotVelX (rad/s)": gyro[:, 0], "RotVelY (rad/s)": gyro[:, 1], "RotVelZ (rad/s)": gyro[:, 2],
        "RotAccX (rad/s2)": rot_acc[:, 0], "RotAccY (rad/s2)": rot_acc[:, 1], "RotAccZ (rad/s2)": rot_acc[:, 2],
        "LinAccRes (m/s2)": lin_acc_res, "RotVelRes (rad/s)": rot_vel_res, "RotAccRes (rad/s2)": rot_acc_res,
    })

    # Preserve auxiliary columns if present
    aux_cols = [c for c in df.columns if c.startswith("mag") or c in ["batt_temp_c", "triggered"]]
    if aux_cols:
        # Reset index to ensure alignment with the new res_df
        aux_data = df[aux_cols].reset_index(drop=True)
        res_df = pd.concat([res_df, aux_data], axis=1)

    return res_df

# =========================================================
# CORE PROCESSING
# =========================================================

def deduplicate_phone_file(input_path, output_path, meta=False):
    """Cleans, optionally deduplicates, and converts a single phone data file."""
    try:
        # Using comment='#' is more robust than skiprows for varying header lengths
        df = pd.read_csv(input_path, comment='#')
    except Exception as e:
        return {"file": input_path.name, "error": str(e)}

    # Normalise time column name
    if "time_ns" not in df.columns:
        if "sensor_time_ns" in df.columns:
            df = df.rename(columns={"sensor_time_ns": "time_ns"})
        else:
            return {"file": input_path.name, "error": f"No time column found in {df.columns}"}

    sensor_cols = ["accelX_g", "accelY_g", "accelZ_g", "gyroX_dps", "gyroY_dps", "gyroZ_dps"]
    for col in sensor_cols:
        if col not in df.columns:
            df[col] = 0

    initial_len = len(df)
    if initial_len == 0:
        return {"file": input_path.name, "initial": 0, "removed": 0, "final": 0}

    # Sort if needed
    if not df["time_ns"].is_monotonic_increasing:
        df = df.sort_values("time_ns").reset_index(drop=True)

    removed_count = 0
    df_cleaned = df

    # Deduplicate only if meta is False
    if not meta:
        times = df["time_ns"].to_numpy(dtype=np.int64)
        vals = df[sensor_cols].to_numpy(dtype=np.float32)

        threshold_ns = 500_000 # 0.5 ms
        diffs = np.diff(times)
        new_group_flags = np.concatenate(([True], diffs >= threshold_ns))
        group_ids = np.cumsum(new_group_flags) - 1
        
        _, group_starts, group_counts = np.unique(group_ids, return_index=True, return_counts=True)
        
        keep_mask = np.ones(initial_len, dtype=bool)
        last_kept_idx = 0
        
        for start, count in zip(group_starts, group_counts):
            if count == 1:
                last_kept_idx = start
                continue
            
            end = start + count
            if start == 0:
                keep_mask[start + 1:end] = False
                last_kept_idx = start
            else:
                # Pick point furthest from the last kept point to capture maximum signal change
                prev_val = vals[last_kept_idx]
                cluster_vals = vals[start:end]
                dists = np.linalg.norm(cluster_vals - prev_val, axis=1)
                keep_idx = start + np.argmax(dists)
                
                keep_mask[start:end] = False
                keep_mask[keep_idx] = True
                last_kept_idx = keep_idx
            
            removed_count += (count - 1)
            
        df_cleaned = df.iloc[keep_mask].reset_index(drop=True)

    # Unit conversion
    df_final = change_units(df_cleaned)
    df_final.to_csv(output_path, index=False)

    stats = {
        "file": input_path.name, "newfile": output_path.name,
        "initial": initial_len, "removed": removed_count, "final": len(df_final),
        "percent_removed": removed_count / initial_len if initial_len > 0 else 0
    }
    if meta:
        stats.update(extract_metadata(input_path))

    return {
        "stats": stats,
        "message": f"{input_path.name}: removed {removed_count} ({stats['percent_removed']:.2%}), now {len(df_final)}."
    }

# =========================================================
# DIRECTORY PROCESSING
# =========================================================

def get_output_name(file_path, log_lookup):
    """Determines the standardized output filename based on logs."""
    if log_lookup.empty:
        return file_path.name
    
    entry = log_lookup[log_lookup["File Name"] == file_path.stem]
    if entry.empty:
        return file_path.name
        
    row = entry.iloc[0]
    speed, repeat = parse_test_name(str(row["Test Name"]))
    config = str(row["Test configuration"])
    phone_id = extract_phone_id(file_path.name)
    
    if speed and repeat:
        return f"{speed}mps_{config}_REPEAT{repeat}_{phone_id}_cleaned.csv"
    
    return file_path.name

def deduplicate_csv_dir(src_dir, dest_dir, log_file, meta=False):
    """Processes all CSVs in a directory using a thread pool."""
    src_dir, dest_dir = Path(src_dir), Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    log_path = Path("test_log_ignore/Data Collection Log.csv")
    log_lookup = pd.read_csv(log_path) if log_path.exists() else pd.DataFrame()

    phone_files = list(src_dir.glob("*.csv"))
    print(f"Processing {len(phone_files)} files from {src_dir}...")

    all_stats = []
    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(deduplicate_phone_file, f, dest_dir / get_output_name(f, log_lookup), meta): f 
            for f in phone_files
        }

        for future in futures:
            res = future.result()
            if "message" in res:
                print(res["message"])
                all_stats.append(res["stats"])
            else:
                print(f"ERROR processing {res['file']}: {res.get('error')}")
                all_stats.append({"file": res["file"], "error": res.get("error")})

    pd.DataFrame(all_stats).to_csv(log_file, index=False)
    print(f"Done. Log saved to {log_file}")

# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":
    # deduplicate_csv_dir(
    #     src_dir=Path("phone_drop_test_data_ignore/phone"),
    #     dest_dir=Path("phone_drop_test_data_ignore/phone_cleaned"),
    #     log_file=Path("test_log_ignore/deduplication_log1.csv"),
    #     meta=False
    # )

    # # Standard drop tests
    # deduplicate_csv_dir(
    #     src_dir="phone_drop_test_data_ignore/phone_01052026",
    #     dest_dir="phone_drop_test_data_ignore/phone_cleaned",
    #     log_file="test_log_ignore/deduplication_log2.csv",
    #     meta=True
    # )

    # Stationary reference data
    deduplicate_csv_dir(
        src_dir="stationary_ignore/raw",
        dest_dir="stationary_ignore/cleaned",
        log_file="test_log_ignore/deduplication_stationary.csv",
        meta=True
    )
