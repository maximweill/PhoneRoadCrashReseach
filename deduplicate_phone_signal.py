import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

def deduplicate_phone_file(input_path, output_path):
    """
    Optimized version using NumPy and vectorized distance calculations.
    Returns a dictionary of statistics for logging.
    """
    try:
        df = pd.read_csv(input_path)
    except Exception as e:
        return {"file": input_path.name, "error": str(e)}
    
    df = df.rename(columns={'sensor_time_ns': 'time_ns'})
    
    if "time_ns" not in df.columns:
        return {"file": input_path.name, "error": f"time_ns column not found {df.columns}"}
    

    initial_len = len(df)
    if initial_len == 0:
        return {"file": input_path.name, "initial": 0, "removed": 0, "final": 0, "percent_removed": 0}

    # 1. Sort by time
    df = df.sort_values("time_ns").reset_index(drop=True)
    
    sensor_cols = [
        "accelX_g", "accelY_g", "accelZ_g", 
        "gyroX_dps", "gyroY_dps", "gyroZ_dps"
    ]
    sensor_cols = [c for c in sensor_cols if c in df.columns]
    
    # Convert to numpy for performance
    times = df["time_ns"].values
    vals = df[sensor_cols].values
    
    # 2. Identify groups of "duplicate" timestamps (difference < 0.5ms)
    # A new group starts whenever the difference to the previous sample is >= threshold
    threshold_ns = 0.5e6
    diffs_init = np.diff(times)
    new_group_flags = np.concatenate(([True], diffs_init >= threshold_ns))
    group_ids = np.cumsum(new_group_flags) - 1
    
    # Find start and end indices for each group
    unique_groups, group_starts, group_counts = np.unique(group_ids, return_index=True, return_counts=True)
    
    # Boolean mask for rows to keep
    keep_mask = np.ones(initial_len, dtype=bool)
    duplicate_groups = np.where(group_counts > 1)[0]
    
    removed_count = 0
    for group_idx in duplicate_groups:
        start = group_starts[group_idx]
        count = group_counts[group_idx]
        end = start + count
        
        if group_idx == 0:
            # For the first group, keep the first one
            keep_mask[start + 1 : end] = False
            removed_count += (count - 1)
            continue
            
        # Previous group's last kept index
        # Since we process groups in order, we can find the last True in keep_mask before 'start'
        prev_indices = np.where(keep_mask[:start])[0]
        if len(prev_indices) == 0:
            # Fallback if somehow no previous sample (shouldn't happen with group_idx > 0)
            keep_mask[start + 1 : end] = False
            removed_count += (count - 1)
            continue
            
        prev_sample_idx = prev_indices[-1]
        prev_val = vals[prev_sample_idx]
        
        cluster_vals = vals[start:end]
        
        # Calculate distances to previous sample
        dists = np.linalg.norm(cluster_vals - prev_val, axis=1)
        
        # Keep the one MOST DISTINCT (largest distance) from the previous sample
        most_distinct_offset = np.argmax(dists)
        
        # Remove all others in the cluster
        group_keep_mask = np.zeros(count, dtype=bool)
        group_keep_mask[most_distinct_offset] = True
        
        keep_mask[start:end] = group_keep_mask
        removed_count += (count - 1)

    df_cleaned = df.iloc[keep_mask].reset_index(drop=True)
    
    final_len = len(df_cleaned)
    percent_removed = (removed_count / initial_len) if initial_len > 0 else 0
    
    if percent_removed < 0.49 and final_len > 1:
        # Calculate delta t
        diffs = np.diff(df_cleaned["time_ns"].values)
        
        plt.figure(figsize=(10, 6))
        plt.hist(diffs, bins=500)
        plt.title(f"Frequency Distribution: {input_path.name}\n(Percent Removed: {percent_removed:.2%})")
        plt.xlabel("diffs (ns)")
        plt.ylabel("Count")
        plt.grid(True, alpha=0.3)
        plt.show()

    df_cleaned.to_csv(output_path, index=False)
    
    stats = {
        "file": input_path.name,
        "initial": initial_len,
        "removed": removed_count,
        "final": final_len,
        "percent_removed": percent_removed
    }
    
    msg = f"  {input_path.name}: Removed {stats['percent_removed']:.2%} samples. now {final_len}/{initial_len}."
    return {"stats": stats, "message": msg}

def main():
    src_dir = Path("phone_drop_test_data_ignore/phone")
    dest_dir = Path("phone_drop_test_data_ignore/phone_cleaned")
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = Path("test_log_ignore/deduplication_log.csv")
    
    phone_files = list(src_dir.glob("*.csv"))
    print(f"Deduplicating {len(phone_files)} files...")
    
    all_stats = []
    
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(deduplicate_phone_file, f, dest_dir / f.name) for f in phone_files]
        
        for future in futures:
            res = future.result()
            if "message" in res:
                print(res["message"])
                all_stats.append(res["stats"])
            elif "error" in res:
                print(f"  {res['file']}: ERROR - {res['error']}")
                all_stats.append({"file": res["file"], "error": res["error"]})
        
    # Save log to CSV
    log_df = pd.DataFrame(all_stats)
    log_df.to_csv(log_file, index=False)
    print(f"\nDeduplication complete. Detailed log saved to {log_file}")

if __name__ == "__main__":
    main()
