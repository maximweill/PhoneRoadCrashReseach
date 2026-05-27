import pandas as pd
import numpy as np
import re
from pathlib import Path
from scipy.interpolate import interp1d
from .core import Context
from .helper import _compute_allan_variance

# =========================================================
# CONSTANTS
# =========================================================

G: float = 9.80665
DEG2RAD: float = np.pi / 180.0

# =========================================================
# PARSING TRANSFORMS
# =========================================================

def normalize_time_column(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    if "time_ns" not in df.columns:
        if "sensor_time_ns" in df.columns:
            df = df.rename(columns={"sensor_time_ns": "time_ns"})
        else:
            raise ValueError("Missing time column")

    return df, ctx

def ensure_sensor_columns(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    cols = [
        "accelX_g", "accelY_g", "accelZ_g","accelMag_g",
        "gyroX_dps", "gyroY_dps", "gyroZ_dps","gyroMag_dps",
    ]

    for c in cols:
        if c not in df.columns:
            df[c] = 0.0

    return df, ctx

def sort_by_time(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    if not df["time_ns"].is_monotonic_increasing:
        df = df.sort_values("time_ns").reset_index(drop=True)

    return df, ctx

def interpolate_outliers(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:
    # 1. Work on a copy to protect the original dataset
    df_clean = df.copy()
    sensor_cols = [col for col in df_clean.columns if "Acc" in col or "Rot" in col]

    # 3. Define your threshold multiplier
    # For MAD, a multiplier between 3 and 5 is standard practice
    threshold_multiplier = 7.4 # ~5sigma 

    for col in sensor_cols:
        # Calculate global median
        median = df_clean[col].median()

        # Calculate global Median Absolute Deviation (MAD)
        mad = (df_clean[col] - median).abs().median()

        # 4. Find deviations that exceed (multiplier * MAD)
        is_outlier = (df_clean[col] - median).abs() > (
            threshold_multiplier * mad
        )
        ctx[f"{col}_MAD"] = mad
        ctx[f"{col}_median"] = median
        ctx[f"{col}_n_outliers"] = int(is_outlier.sum())

        # 5. Mask outliers with NaN and apply linear interpolation
        df_clean.loc[is_outlier, col] = np.nan
        df_clean[col] = df_clean[col].interpolate(
            method="linear", limit_direction="both"
        )

    return df_clean, ctx

def deduplicate_DEPRECATED(
    df: pd.DataFrame,
    ctx: Context,
    threshold_ns: int = 500_000,
) -> tuple[pd.DataFrame, Context]:

    sensor_cols = [
        "accelX_g", "accelY_g", "accelZ_g",
        "gyroX_dps", "gyroY_dps", "gyroZ_dps",
    ]

    times = df["time_ns"].to_numpy(dtype=np.int64)
    vals = df[sensor_cols].to_numpy(dtype=np.float32)

    diffs = np.diff(times)
    new_group = np.concatenate(([True], diffs >= threshold_ns))
    group_ids = np.cumsum(new_group) - 1

    _, starts, counts = np.unique(
        group_ids,
        return_index=True,
        return_counts=True,
    )

    keep = np.ones(len(df), dtype=bool)

    last_kept = 0
    removed = 0

    for start, count in zip(starts, counts):

        if count == 1:
            last_kept = int(start)
            continue

        end = int(start + count)

        if start == 0:
            keep[start + 1:end] = False
            last_kept = int(start)

        else:
            prev = vals[last_kept]
            cluster = vals[start:end]

            dists = np.linalg.norm(cluster - prev, axis=1)
            keep_idx = int(start + np.argmax(dists))

            keep[start:end] = False
            keep[keep_idx] = True
            last_kept = keep_idx

        removed += count - 1

    df = df.loc[keep].reset_index(drop=True)

    ctx["removed_rows"] = removed
    ctx["initial_rows"] = len(keep)
    ctx["final_rows"] = len(df)

    return df, ctx

def accelerometer_based_timestamps(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    sensor_cols = [
        "accelX_g",
        "accelY_g",
        "accelZ_g",
    ]

    vals = df[sensor_cols].to_numpy(dtype=np.float32)

    keep = np.ones(len(df), dtype=bool)

    # compare each sample with previous sample
    same_as_prev = np.all(vals[1:] == vals[:-1], axis=1)

    keep[1:] = ~same_as_prev

    removed = int(np.sum(~keep))

    df = df.loc[keep].reset_index(drop=True)

    ctx["acc_removed_rows"] = removed
    ctx["acc_initial_rows"] = len(keep)
    ctx["acc_final_rows"] = len(df)

    return df, ctx

def deduplicate(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    keep = ~df["time_ns"].duplicated(keep="first")

    removed = int((~keep).sum())

    df = df.loc[keep].reset_index(drop=True)
    
    ctx["dedup_removed_rows"] = removed
    ctx["dedup_initial_rows"] = len(keep)
    ctx["dedup_final_rows"] = len(df)

    return df, ctx

def convert_units(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    t = df["time_ns"].to_numpy(dtype=np.float64) / 1e9

    # Components only for vector math
    accel_vec = (
        df[["accelX_g", "accelY_g", "accelZ_g"]]
        .to_numpy(dtype=np.float32)
        * G
    )

    gyro_vec = (
        df[["gyroX_dps", "gyroY_dps", "gyroZ_dps"]]
        .to_numpy(dtype=np.float32)
        * DEG2RAD
    )

    # Recompute magnitudes to be sure they are consistent with components
    accel_mag = np.linalg.norm(accel_vec, axis=1)
    gyro_mag = np.linalg.norm(gyro_vec, axis=1)

    # Rotational acceleration: derivative of the angular velocity VECTOR
    rot_acc_vec = np.gradient(gyro_vec, t, axis=0)
    rot_acc_mag = np.linalg.norm(rot_acc_vec, axis=1)

    # Preserve columns that might have been added by previous transforms
    preserve = ["trigger", "axis", "triggered","batt_temp_c"]
    extra_cols = {c: df[c].to_numpy() for c in preserve if c in df.columns}

    df = pd.DataFrame({
        "Time (s)": t,

        "LinAccX (m/s2)": accel_vec[:, 0],
        "LinAccY (m/s2)": accel_vec[:, 1],
        "LinAccZ (m/s2)": accel_vec[:, 2],
        "LinAccRes (m/s2)": accel_mag,

        "RotVelX (rad/s)": gyro_vec[:, 0],
        "RotVelY (rad/s)": gyro_vec[:, 1],
        "RotVelZ (rad/s)": gyro_vec[:, 2],
        "RotVelRes (rad/s)": gyro_mag,

        "RotAccX (rad/s2)": rot_acc_vec[:, 0],
        "RotAccY (rad/s2)": rot_acc_vec[:, 1],
        "RotAccZ (rad/s2)": rot_acc_vec[:, 2],
        "RotAccRes (rad/s2)": rot_acc_mag,
        
        **extra_cols
    })

    return df, ctx

def parse_test_metadata(df: pd.DataFrame, ctx: Context):
    """
    Extract structured fields from raw log rows.
    """
    df["config"] = df["Test_configuration"].fillna("unknown")
    df["target_speed_mps"] = pd.to_numeric(df["Target_speed"], errors="coerce")

    return df, ctx

def extract_repeat_from_test_name(df: pd.DataFrame, ctx: Context):
    """
    Extracts repeat information (V1, REPEAT1, etc.) from the Test_name column.
    """
    df["repeat"] = (
        df["Test_Name"]
        .astype(str)
        .str.extract(r'(?i)\bv(\d+)\b', expand=False)
        .astype("Int64")  # keeps it nullable-safe
    )

    return df, ctx

def rename_continuous_test_files(df: pd.DataFrame, ctx: Context):
    
    # Ensure ctx has a bugs list
    ctx.setdefault("bugs", [])

    # Find duplicated filenames
    duplicated_mask = df["File_Name"].duplicated(keep=False)

    for file_name, group_idx in df[duplicated_mask].groupby("File_Name").groups.items():

        if "continuous" in file_name.lower():

            for trigger_num, idx in enumerate(group_idx, start=1):

                # Preserve extension if present
                if "." in file_name:
                    base, ext = file_name.rsplit(".", 1)
                    new_name = f"{base}_trigger{trigger_num}.{ext}"
                else:
                    new_name = f"{file_name}_trigger{trigger_num}"

                df.at[idx, "File_Name"] = new_name

        else:
            ctx["bugs"].append(
                f"Duplicate non-continuous filename found: {file_name}"
            )

    return df, ctx

def clean_speed_column(df: pd.DataFrame, ctx: Context):
    # sometimes "Speed (m/s)" or "Speed" or empty
    for col in df.columns:
        if "speed" in col.lower() and "target" not in col.lower():
            df["measured_speed_mps"] = pd.to_numeric(df[col], errors="coerce")
            break

    return df, ctx

# =========================================================
# Logging TRANSFORMS
# =========================================================

def drop_failed_rows(df: pd.DataFrame, ctx: Context):
    df = df[df["Successful"]=="TRUE"]
    return df, ctx

def drop_empty_rows(df: pd.DataFrame, ctx: Context):
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")
    return df, ctx

def normalize_column_names(df: pd.DataFrame, ctx: Context):
    # Conservative cleaning: strip and remove only truly problematic chars
    # Keeps spaces, parens, and slashes for unit consistency
    df.columns = (
        df.columns
        .str.strip()
        .str.replace(r"[^a-zA-Z0-9 ()//_]", "", regex=True)
    )
    return df, ctx

def normalize_log_columns(df: pd.DataFrame, ctx: Context):
    # Aggressive cleaning for logbooks: convert to snake_case
    df.columns = (
        df.columns
        .str.strip()
        .str.replace(r"[\s/-]+", "_", regex=True)
        .str.replace(r"[^a-zA-Z0-9_]", "", regex=True)
        .str.replace(r"__+", "_", regex=True)
        .str.strip("_")
    )
    return df, ctx

def extract_phone_id(df: pd.DataFrame, ctx: Context):
    if "File_Name" in df.columns:
        df["phone_id"] = df["File_Name"].str.extract(r"(Phone\d+)")
    return df, ctx

# =========================================================
# CONTINUOUS DROPS & CALIBRATION TRANSFORMS
# =========================================================

def remove_accidental_triggers(
    df: pd.DataFrame, 
    ctx: Context,
    min_interval_s: float = 150.0
) -> tuple[pd.DataFrame, Context]:
    """
    Cleans the 'triggered' column to produce a stable 'trigger' column.
    Sequence:
    Trigger 0: Start of file (Calibration only, NO drop)
    Trigger 1: First increment (Drop 1 then Calibration 1)
    Trigger 2: Second increment (Drop 2 then Calibration 2)
    ...
    """
    if "triggered" not in df.columns:
        ctx["error"] = "no triggered column"
        return df, ctx

    # Find points where triggered increments
    raw_triggered = df["triggered"].to_numpy()
    diff = np.diff(raw_triggered, prepend=raw_triggered[0])
    inc_indices = np.where(diff > 0)[0]
    
    if len(inc_indices) == 0:
        ctx["error"] = " no increments, everything is trigger 0"
        df["trigger"] = 0
        return df, ctx

    # Time column (assuming convert_units has run)
    time_col = "Time (s)"
    times = df[time_col].to_numpy()
    
    # Filter accidental increments (if T_i - T_{i-1} < 120s)
    valid_inc_indices = []
    last_valid_time = times[0]-100 #not inf for the same of int()
    trigger_durations = []
    
    for idx in inc_indices:
        current_time = times[idx]
        dt = current_time - last_valid_time

        trigger_durations.append(int(dt))

        if dt >= min_interval_s:
            valid_inc_indices.append(idx)
            last_valid_time = current_time
    

    if len(valid_inc_indices)>=11: #the maximum number of triggers should be 10
        valid_inc_indices = valid_inc_indices[0:10]
        ctx["trigger_durations"] = trigger_durations[0:10]
    else:
        ctx["trigger_durations"] = trigger_durations

    new_trigger = np.zeros(len(df), dtype=int)
    for i, start_idx in enumerate(valid_inc_indices, 1):
        new_trigger[start_idx:] = i
        
    df["trigger"] = new_trigger
    
    ctx["original_trigger_count"] = len(inc_indices)
    ctx["valid_trigger_count"] = len(valid_inc_indices)
    
    return df, ctx

def extract_drops(
    df: pd.DataFrame, 
    ctx: Context,
    except_s: float = 120.0
) -> tuple[pd.DataFrame, Context]:
    """
    Extracts a window of data at the START of each trigger increment (T1+).
    T0 has no drop.
    """
    if "trigger" not in df.columns:
        return df, ctx

    # Increments happen at the start of the drop
    change_mask = df["trigger"].diff() > 0
    trigger_indices = df.index[change_mask].tolist()
    trigger_levels = df.loc[change_mask, "trigger"].tolist()
    
    segments = []
    for idx,next_idx, level in zip(trigger_indices,trigger_indices[1:], trigger_levels):
        # Window starts at increment and goes for window_s
        t_start = df.loc[idx, "Time (s)"]
        t_end = df.loc[next_idx, "Time (s)"] - except_s
        if t_end <= t_start:
            continue
        
        seg = df[(df["Time (s)"] >= t_start) & (df["Time (s)"] < t_end)].copy()
        seg["trigger"] = level
        segments.append(seg)
        
    if not segments:
        return pd.DataFrame(columns=df.columns), ctx
        
    df = pd.concat(segments).reset_index(drop=True)
    return df, ctx

def extract_calibration(
    df: pd.DataFrame, 
    ctx: Context,
    min_stationary_s: float = 15.0
) -> tuple[pd.DataFrame, Context]:
    """
    Finds calibration axes in each trigger segment.
    Trigger 0: Search from start of file until first increment.
    Trigger 1+: Search within the segment (after the drop).
    """
    if "trigger" not in df.columns:
        return df, ctx

    accel_cols = ["LinAccX (m/s2)", "LinAccY (m/s2)", "LinAccZ (m/s2)"]
    gyro_mag_col = "RotVelRes (rad/s)"
    gyro_threshold = 5.0 * (np.pi / 180.0)
    
    stationary = df[gyro_mag_col] < gyro_threshold
    
    final_segments = []
    calibration_stats = []
    
    # Process each trigger level segment
    for level in sorted(df["trigger"].unique()):
        seg_df = df[df["trigger"] == level]

        seg_stat = stationary.loc[seg_df.index].to_numpy()
        
        # Find blocks in this segment, searching from the end (backwards)
        diff_stat = np.diff(seg_stat.astype(int), prepend=0, append=0)
        starts = np.where(diff_stat == 1)[0]
        ends = np.where(diff_stat == -1)[0]
        
        blocks = list(zip(starts, ends))
        blocks.reverse() 
        
        found_axes = {}
        for b_start, b_end in blocks:
            block_df = seg_df.iloc[b_start:b_end]
            duration_s = block_df["Time (s)"].iloc[-1] - block_df["Time (s)"].iloc[0]
            
            if duration_s < min_stationary_s:
                continue
                
            mean_accel = block_df[accel_cols].mean().to_numpy()
            max_gyro = block_df[gyro_mag_col].max()
            
            # Determine axis
            axis_idx = np.argmax(np.abs(mean_accel))
            axis_sign = np.sign(mean_accel[axis_idx])
            axis_name = ["x", "y", "z"][axis_idx]
            if axis_sign < 0:
                axis_name = "-" + axis_name
                
            if axis_name not in found_axes:
                block_df = block_df.copy()
                block_df["axis"] = axis_name
                block_df["trigger"] = level
                found_axes[axis_name] = block_df
                
                calibration_stats.append({
                    "trigger": int(level),
                    "axis": axis_name,
                    "max_gyro_rads": int(max_gyro),
                    "duration_s": int(duration_s)
                })
                
            if len(found_axes) == 6:
                break
        
        if found_axes:
            for ax in ["x", "-x", "y", "-y", "z", "-z"]:
                if ax in found_axes:
                    final_segments.append(found_axes[ax])
                    
    ctx["calibration_summary"] = calibration_stats
    
    if not final_segments:
        return pd.DataFrame(columns=df.columns.tolist() + ["axis"]), ctx
        
    df = pd.concat(final_segments).reset_index(drop=True)
    return df, ctx

# =========================================================
# CHARACTERISTICS TRANSFORMS
# =========================================================

def compute_sampling_rate_stats(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    time_col = "Time (s)" if "Time (s)" in df.columns else "time_ns"
    if time_col not in df.columns:
        return df, ctx
    
    t = df[time_col].to_numpy()
    if len(t) < 2:
        return df, ctx
        
    diffs = np.diff(t)
    if time_col == "time_ns":
        diffs = diffs * 1e-9
        
    diffs = diffs[diffs > 0]
    if len(diffs) == 0:
        return df, ctx
        
    freqs = 1.0 / diffs
    ctx["fs_mean"] = np.mean(freqs)
    ctx["fs_median"] = np.median(freqs)
    q75, q25 = np.percentile(freqs, [75, 25])
    ctx["fs_iqr"] = q75 - q25
    
    return df, ctx

def compute_battery_stats(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    if "batt_temp_c" in df.columns:
        temps = df["batt_temp_c"].dropna()
        if not temps.empty:
            ctx["battery_temp_c_mean"] = temps.mean()
            ctx["battery_temp_c_median"] = temps.median()
            q75, q25 = np.percentile(temps, [75, 25])
            ctx["battery_temp_c_iqr"] = q75 - q25
        else:
            ctx["battery_temp_c_mean"] = np.nan
            ctx["battery_temp_c_median"] = np.nan
            ctx["battery_temp_c_iqr"] = np.nan
    else:
        ctx["battery_temp_c_mean"] = np.nan
        ctx["battery_temp_c_median"] = np.nan
        ctx["battery_temp_c_iqr"] = np.nan
    return df, ctx

def compute_magnetic_stats(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    for axis in ["X", "Y", "Z"]:
        col = f"mag{axis}_uT"
        if col in df.columns:
            ctx[f"initial_{col}"] = df[col].iloc[:100].mean()
    return df, ctx

def compute_sensor_max_stats(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    sensor_cols = [
        "LinAccX (m/s2)", "LinAccY (m/s2)", "LinAccZ (m/s2)", "LinAccRes (m/s2)",
        "RotVelX (rad/s)", "RotVelY (rad/s)", "RotVelZ (rad/s)", "RotVelRes (rad/s)",
        "RotAccX (rad/s2)", "RotAccY (rad/s2)", "RotAccZ (rad/s2)", "RotAccRes (rad/s2)",
        "magX_uT", "magY_uT", "magZ_uT", "magMag_uT"
    ]
    for col in sensor_cols:
        if col in df.columns:
            ctx[f"max_{col}"] = max(df[col].max(), -df[col].min())
    return df, ctx

def create_characteristics_summary(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    # 1. Extract phone id from filename if not already present in ctx
    if "phone_id" not in ctx:
        match = re.search(r"(Phone\d+)", ctx["input_path"].name)
        ctx["phone_id"] = match.group(1) if match else "Unknown"

    # Define standard metrics we want to keep from the top-level context
    characteristics_keys = [
        "phone_id", "fs_mean", "fs_median", "fs_iqr", 
        "battery_temp_c_mean", "battery_temp_c_median", "battery_temp_c_iqr", 
        "initial_magX_uT", "initial_magY_uT", "initial_magZ_uT"
    ]
    
    # This is the ONLY dictionary we will use to build the DataFrame row
    final_row = {}
    
    final_row["input_path"] = ctx["input_path"]
    
    # 2. Extract individual metadata dictionary items as independent top-level columns
    metadata = ctx.get("metadata", {})
    for meta_key, meta_val in metadata.items():
        final_row[meta_key] = meta_val  # Becomes independent columns: 'Device', 'Date', etc.

    # 3. Pull explicitly allowed characteristics from top-level ctx
    for key in characteristics_keys:
        if key in ctx:
            final_row[key] = ctx[key]

    # 4. Pull dynamic "max_" columns from top-level ctx, ignoring anything else
    for key, val in ctx.items():
        if key.startswith("max_"):
            final_row[key] = val

    return df, final_row

def aggregate_characteristics_by_phone(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    """
    Groups the collected characteristics by Phone ID and applies aggregation rules.
    """
    print("Aggregating characteristics by Phone ID...")
    
    if "phone_id" not in df.columns:
        print("NO PHONE ID in individual char")
        return df, ctx
        
    avg_metrics = ["fs_mean", "fs_median", "fs_iqr", 
                   "battery_temp_c_mean", "battery_temp_c_median", "battery_temp_c_iqr"]
    
    # Metadata columns we want to grab the mode of
    metadata_cols = ["Device", "Accelerometer", "Gyroscope", "Magnetometer", "Date"]
    
    # Sensor max columns we want the global max of
    max_cols = [c for c in df.columns if c.startswith("max_")]
    
    agg_rules = {}
    for col in avg_metrics:
        if col in df.columns:
            agg_rules[col] = "mean"
            
    for col in max_cols:
        agg_rules[col] = "max"
        
    for col in metadata_cols:
        if col in df.columns:
            # Code to safely grab the most common string value (the mode)
            agg_rules[col] = lambda x: x.dropna().mode().iloc[0] if not x.dropna().mode().empty else np.nan

    # Group by the phone_id column
    df_aggregated = df.groupby("phone_id").agg(agg_rules).reset_index()
    
    # Rounding for readability
    numeric_cols = df_aggregated.select_dtypes(include=[np.number]).columns
    df_aggregated[numeric_cols] = df_aggregated[numeric_cols].round(3)

    return df_aggregated, ctx

def add_phyphox_data(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    """
    Joins the aggregated characteristics with sensor range and rate data from Phyphox.
    """
    phyphox_path = Path("phyphox_data/fast_data/devices.parquet")
    if not phyphox_path.exists():
        print(f"Warning: Phyphox data not found at {phyphox_path}")
        return df, ctx

    phyphox_df = pd.read_parquet(phyphox_path)
    
    # Columns we want to keep from Phyphox data
    phyphox_cols = [
        "model",
        "accelerometer_rate", "accelerometer_range",
        "gyroscope_rate", "gyroscope_range",
        "magnetometer_rate", "magnetometer_range",
        "pressure_sensor_rate", "pressure_sensor_range"
    ]
    phyphox_df = phyphox_df[phyphox_cols]
    df[["Brand", "Model"]] = df["Device"].str.split(" ", n=1, expand=True)

    # Join on Device == model
    if "Model" in df.columns:
        df = df.merge(phyphox_df, left_on="Model", right_on="model", how="left")
        # Optionally drop the redundant 'model' column
        if "Model" in df.columns:
            df = df.drop(columns=["model"])
    else:
        print("Warning: 'Device' column not found in characteristics. Cannot join with Phyphox data.")

    return df, ctx

def resample_reference(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    """
    Resamples reference data to match a specific phone's sampling frequency.
    Target frequency is looked up from phone_characteristics_aggregated.csv.
    Uses linear interpolation.
    """
    chars_path = ctx.get("phone_characteristics_aggregated")
    if not chars_path:
        return df, ctx

    if not chars_path.exists():
        print(f"Warning: Characteristics file not found at {chars_path}")
        return df, ctx

    chars_df = pd.read_csv(chars_path)

    # Extract phone ID from reference filename
    filename = ctx["input_path"].name
    match = re.search(r"(Phone\d+)", filename)
    if not match:
        print(f"Warning: Could not extract Phone ID from {filename}")
        return df, ctx

    phone_id = match.group(1)

    # Get target frequency
    phone_info = chars_df[chars_df["phone_id"] == phone_id]
    if phone_info.empty:
        print(f"Warning: No characteristics found for {phone_id} in {chars_path}")
        return df, ctx

    fs = phone_info["fs_median"].iloc[0]
    dt_new = 1.0 / fs

    # Linear interpolation resampling
    t_orig = df["Time (s)"].to_numpy()

    # Create new time vector covering the same range
    t_new = np.arange(t_orig[0], t_orig[-1], dt_new)

    resampled_data = {"Time (s)": t_new}

    # Resample each sensor column
    for col in df.columns:
        if col == "Time (s)": continue

        # interp1d for linear interpolation
        f = interp1d(t_orig, df[col].to_numpy(), kind='linear', fill_value="extrapolate")
        resampled_data[col] = f(t_new)

    df_new = pd.DataFrame(resampled_data)

    return df_new, ctx

# =========================================================
# FRAMING TRANSFORMS
# =========================================================
from scipy import signal

def ref_timestamps_matching(df: pd.DataFrame, ctx: Context):
    ref_time = ctx["ref_df"]["Time (s)"]

    t_orig = df["Time (s)"]
    t_new = ref_time.copy()

    resampled_data = {"Time (s)": ref_time.copy()}

    # Resample each sensor column
    for col in df.columns:
        if col == "Time (s)":
            continue

        # interp1d for linear interpolation
        f = interp1d(t_orig, df[col].to_numpy(), kind='linear', fill_value="extrapolate")
        resampled_data[col] = f(t_new)

    df_new = pd.DataFrame(resampled_data)

    ctx["len_ratio"] = len(df)/len(df_new)

    return df_new, ctx

def compute_lag(df: pd.DataFrame, ctx: Context):
    ref_df = ctx["ref_df"]

    sig_col = ctx.get("signal_col", "RotVelX (rad/s)")
    ref_sig_col = ctx.get("ref_signal_col", "RotVelX (rad/s)")

    sig_p = np.nan_to_num(df[sig_col].to_numpy())
    sig_r = np.nan_to_num(ref_df[ref_sig_col].to_numpy())

    sig_p = (sig_p - sig_p.mean()) / (sig_p.std() + 1e-9)
    sig_r = (sig_r - sig_r.mean()) / (sig_r.std() + 1e-9)

    corr = signal.correlate(sig_p, sig_r, mode="full", method="fft")
    lags = signal.correlation_lags(len(sig_p), len(sig_r), mode="full")

    ctx["lag_idx"] = int(lags[np.argmax(corr)])
    return df, ctx

def align_to_reference(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    lag = int(ctx.get("lag_idx", 0))
    ref_df = ctx["ref_df"]

    if lag >= 0:
        start_df = lag
        start_ref = 0
    else:
        start_df = 0
        start_ref = -lag

    overlap = min(
        len(df) - start_df,
        len(ref_df) - start_ref,
    )

    if overlap <= 0:
        raise ValueError("No overlap between signals")

    df_aligned = df.iloc[start_df:start_df + overlap].copy()

    t_df = df_aligned["Time (s)"].iloc[0]
    t_ref = ref_df["Time (s)"].iloc[start_ref]

    offset = t_ref - t_df
    df_aligned["Time (s)"] += offset

    ctx["offset"] = offset

    return df_aligned, ctx

def trim_stationary(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    threshold = ctx.get("impact_threshold", 11)

    t = df["Time (s)"].to_numpy()
    acc = df["LinAccRes (m/s2)"].to_numpy()

    mask = acc > threshold
    impact_times = t[mask]

    t_start, t_end = t[0], t[-1]
    t_mid = (t_start + t_end) / 2

    first = impact_times[impact_times < t_mid]
    second = impact_times[impact_times > t_mid]

    start_time = first[-1] if len(first) else t_start
    end_time = second[0] if len(second) else t_end

    start_time += ctx.get("buffer_after", 600)
    end_time -= ctx.get("buffer_before", 60)

    df = df[(df["Time (s)"] > start_time) & (df["Time (s)"] < end_time)].copy()

    if len(df) > 0:
        df["Time (s)"] -= df["Time (s)"].iloc[0]

    return df, ctx

def trim_reference(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    start_time = -0.1
    end_time = 0.4

    df = df[(df["Time (s)"] > start_time) & (df["Time (s)"] < end_time)].copy()

    return df, ctx


def match_indices_ref(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    ref_df = ctx["ref_df"].reset_index(drop=True)
    df = df.reset_index(drop=True)
    ctx["len_match"] = len(ref_df) - len(df)
    time_diff = ref_df["Time (s)"]-df["Time (s)"]
    ctx["time_match_mean"] = time_diff.mean()
    ctx["time_match_iqr"] = (
        time_diff.quantile(0.75)
        - time_diff.quantile(0.25)
    )


    new_df = pd.concat(
        [
            ref_df.add_suffix("_ref"),
            df.add_suffix("_framed"),
        ],
        axis=1,
    )
    ctx["skip_col"] = []
    for col in df:
        if col not in ref_df.columns:
            ctx["skip_col"].append(col)
            new_df.drop(columns=[f"{col}_framed"], inplace=True)
            continue
        
        new_df[f"{col}_diff"] = new_df[f"{col}_ref"]-new_df[f"{col}_framed"]
    return new_df, ctx
    

def drop_time(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:
    for col in df.columns:
        if "Time" in col:
            df.drop(columns=[col], inplace=True)
    return df, ctx
# =========================================================
# AGREEMENT TRANSFORMS
# =========================================================
def reset_index(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    df_rest = df.reset_index(drop=True)
    ctx["bug"] = [] #create a bugs catcher
    return df_rest, ctx 

def ignore_saturated(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    """
    Identifies and masks saturated sensor readings based on device-specific ranges.
    """
    # 0. Find the characteristics file
    # Fully rely on the drop characteristics file
    chars_path = Path("data_processing_gitignore/phone_characteristics/aggregated/characteristics_drops.csv")
            
    if not chars_path.exists():
        print(f"Warning: Characteristics file not found at {chars_path}. Skipping ignore_saturated.")
        return df, ctx

    # 1. Extract phone_id from filename
    input_path = ctx.get("input_path")
    if not input_path:
        print("Warning: input_path not found in context. Skipping ignore_saturated.")
        return df, ctx
        
    filename = input_path.name
    match = re.search(r"(Phone\d+)", filename)
    if not match:
        print(f"Warning: Could not extract Phone ID from {filename}. Skipping ignore_saturated.")
        return df, ctx
    phone_id = match.group(1)

    # 2. Load characteristics data
    chars_df = pd.read_csv(chars_path)
    phone_info = chars_df[chars_df["phone_id"] == phone_id]
    if phone_info.empty:
        print(f"Warning: No characteristics found for {phone_id} in {chars_path}. Skipping ignore_saturated.")
        return df, ctx

    # 3. Get ranges (handling potential NaNs)
    acc_range = phone_info["accelerometer_range"].iloc[0]
    gyro_range = phone_info["gyroscope_range"].iloc[0]
    
    if pd.isna(acc_range) or pd.isna(gyro_range):
        print(f"Warning: Missing range data for {phone_id}. Skipping ignore_saturated.")
        return df, ctx

    # 4. Define thresholds (using a 2% tolerance to account for near-saturation effects)
    acc_thresh = acc_range * 0.98
    gyro_thresh = gyro_range * 0.98

    # 5. Identify saturated indices
    # We check the '_framed' columns for saturation
    acc_cols = ["LinAccX (m/s2)", "LinAccY (m/s2)", "LinAccZ (m/s2)"]
    gyro_cols = ["RotVelX (rad/s)", "RotVelY (rad/s)", "RotVelZ (rad/s)"]

    acc_mask = pd.Series(False, index=df.index)
    for col in acc_cols:
        c = f"{col}_framed"
        if c in df.columns:
            acc_mask |= df[c].abs() > acc_thresh

    gyro_mask = pd.Series(False, index=df.index)
    for col in gyro_cols:
        c = f"{col}_framed"
        if c in df.columns:
            gyro_mask |= df[c].abs() > gyro_thresh

    # 6. Define effected column groups
    # Saturated accel affects components and magnitude
    acc_group = acc_cols + ["LinAccRes (m/s2)"]
    
    # Saturated gyro affects components, magnitude, and all rotational accelerations
    gyro_group = gyro_cols + [
        "RotVelRes (rad/s)", 
        "RotAccX (rad/s2)", "RotAccY (rad/s2)", "RotAccZ (rad/s2)", "RotAccRes (rad/s2)"
    ]

    # Magnetometer columns
    mag_cols = [c.replace("_framed", "") for c in df.columns if "mag" in c.lower() and c.endswith("_framed")]

    # 7. Apply masking (mask _framed, _ref, and _diff for the same indices)
    for col_stem in acc_group:
        for suffix in ["_framed", "_ref", "_diff"]:
            c = f"{col_stem}{suffix}"
            if c in df.columns:
                df.loc[acc_mask, c] = np.nan

    for col_stem in gyro_group:
        for suffix in ["_framed", "_ref", "_diff"]:
            c = f"{col_stem}{suffix}"
            if c in df.columns:
                df.loc[gyro_mask, c] = np.nan

    # Mask mag if either sensor is saturated
    for col_stem in mag_cols:
        for suffix in ["_framed", "_ref", "_diff"]:
            c = f"{col_stem}{suffix}"
            if c in df.columns:
                df.loc[acc_mask | gyro_mask, c] = np.nan

    ctx["acc_saturated_samples"] = int(acc_mask.sum())
    ctx["gyro_saturated_samples"] = int(gyro_mask.sum())

    return df, ctx


def compute_n(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    ctx["n"] = len(df)
    # Also record valid sample counts for each diff column
    for col in df:
        if col.endswith("_diff"):
            ctx[f"{col}_n_valid"] = int(df[col].notna().sum())
    return df, ctx

def compute_mae_from_ideal(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    for col in df:
        if col.endswith("_diff"):
            # pandas mean() skips NaNs by default
            ctx[f"{col}_mae"] = df[col].abs().mean()
    return df, ctx

from scipy.stats import pearsonr
def compute_pearson_correlation(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    for col in df:
        if col.endswith("_diff"):
            stem = col.replace("_diff", "")
            
            # Pairwise drop NaNs
            valid_mask = df[f"{stem}_ref"].notna() & df[f"{stem}_framed"].notna()
            if valid_mask.sum() < 2:
                continue
                
            r, p = pearsonr(df.loc[valid_mask, f"{stem}_ref"], df.loc[valid_mask, f"{stem}_framed"])
            ctx[f"{stem}_pearson_r"] = r
            ctx[f"{stem}_pearson_p"] = p

    return df, ctx

import statsmodels.api as sm
def compute_trendline_regression(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    for col in df:
        if col.endswith("_diff"):
            stem = col.replace("_diff", "")

            # Pairwise drop NaNs
            valid_mask = df[f"{stem}_ref"].notna() & df[f"{stem}_framed"].notna()
            if valid_mask.sum() < 2:
                continue

            x = df.loc[valid_mask, f"{stem}_ref"]
            y = df.loc[valid_mask, f"{stem}_framed"]

            X = sm.add_constant(x)
            model = sm.OLS(y, X).fit()

            # parameters
            intercept = model.params.iloc[0]
            slope = model.params.iloc[1]

            # predictions
            y_hat = model.predict(X)

            # RMSE of regression fit
            rmse = np.sqrt(np.mean((y - y_hat) ** 2))

            # store metrics
            ctx[f"{stem}_slope"] = slope
            ctx[f"{stem}_intercept"] = intercept
            ctx[f"{stem}_trend_rmse"] = rmse
            ctx[f"{stem}_r2"] = model.rsquared

    return df, ctx

import pingouin as pg
import numpy as np
import pandas as pd
import warnings

def compute_icc(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:

    for col in df:
        if col.endswith("_diff"):
            stem = col.replace("_diff", "")
            ref_col = f"{stem}_ref"
            framed_col = f"{stem}_framed"

            # Check if columns actually exist in the DataFrame
            if ref_col not in df.columns or framed_col not in df.columns:
                ctx["bug"].append(f"Skipped {col}: Matching ref/framed columns not found.")
                continue

            # Pairwise drop NaNs
            valid_mask = df[ref_col].notna() & df[framed_col].notna()
            n = valid_mask.sum()
            
            if n < 2:
                ctx["bug"].append(f"icc failed {col}: Insufficient data points (n={n}).")
                continue

            x = df.loc[valid_mask, ref_col].to_numpy()
            y = df.loc[valid_mask, framed_col].to_numpy()

            # Pre-flight check: Zero variance breaks matrix math
            if np.var(x) == 0 and np.var(y) == 0:
                ctx["bug"].append(f"icc failed {col}: Zero variance in signals (n={n}).")
                continue

            # --- build long format ---
            data = pd.DataFrame({
                "targets": np.repeat(np.arange(n), 2),
                "raters": ["ref", "framed"] * n,
                "values": np.ravel(np.column_stack([x, y]))
            })

            # Catch run-time issues safely
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                try:
                    icc_result = pg.intraclass_corr(
                        data=data,
                        targets="targets",
                        raters="raters",
                        ratings="values"
                    )
                except Exception as e:
                    ctx["bug"].append(f"icc failed {col}: Pingouin exception -> {str(e)}")
                    continue

            icc_row = icc_result[icc_result["Type"].isin(["ICC(A,1)", "ICC(A,k)"])]

            if icc_row.empty:
                warning_msg = f" ({w[0].message})" if w else ""
                ctx["bug"].append(f"icc failed {col}: 'ICC(A,1)'/'ICC(A,k)' missing from output{warning_msg}.")
                continue

            # 'ICC' remains the same, but 'CI95%' is now 'CI95'
            icc_value = icc_row.iloc[0]["ICC"]
            ci95 = icc_row.iloc[0]["CI95"] 

            if np.isnan(icc_value):
                ctx["bug"].append(f"icc failed {col}: Result is NaN.")
                continue

            ctx[f"{stem}_icc"] = icc_value
            ctx[f"{stem}_icc_ci95_lower"] = ci95[0]
            ctx[f"{stem}_icc_ci95_upper"] = ci95[1]

    return df, ctx


import numpy as np
from scipy.stats import ttest_1samp
def compute_bland_altman(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    """
    Bland Altman agreement analysis per channel.
    """

    for col in df:
        if col.endswith("_diff"):
            stem = col.replace("_diff", "")

            diff = df[col].dropna().to_numpy()

            if len(diff) < 2:
                continue

            bias = np.mean(diff)
            sd = np.std(diff)

            # Use the already dropped-nan diff for the t-test
            t_stat, p = ttest_1samp(diff, 0)
            ctx[f"{col}_bias_t_stat"] = t_stat
            ctx[f"{col}_bias_p"] = p

            loa_upper = bias + 1.96 * sd
            loa_lower = bias - 1.96 * sd

            ctx[f"{stem}_ba_sd"] = sd
            ctx[f"{stem}_ba_loa_upper"] = loa_upper
            ctx[f"{stem}_ba_loa_lower"] = loa_lower

    return df, ctx

# =========================================================
# ALLAN VARIANCE TRANSFORMS
# =========================================================


def calculate_allan_variance_transform(df: pd.DataFrame, ctx: Context,both = False) -> tuple[pd.DataFrame, Context]:
    """
    Computes Allan variance for relevant columns in the dataframe.
    """
    filename = ctx["input_path"].name

    # Determine columns to analyze based on file prefix
    if not both and (filename.startswith("accel") or filename.startswith("gyro")):
        if filename.startswith("accel"):
            cols = ["LinAccX (m/s2)", "LinAccY (m/s2)", "LinAccZ (m/s2)", "LinAccRes (m/s2)"]
        elif filename.startswith("gyro"):
            cols = ["RotVelX (rad/s)", "RotVelY (rad/s)", "RotVelZ (rad/s)", "RotVelRes (rad/s)"]
    else:
        # Default to all numeric columns except Time, triggered, and battery info
        cols = [c for c in df.columns if any(x in c for x in ["Acc", "Vel", "Rot"])]

    # Filtering existing columns
    cols = [c for c in cols if c in df.columns]

    if not cols:
        print(f"  Skipping {filename}: No relevant columns found.")
        return None, ctx

    # Sampling interval
    t = df["Time (s)"].to_numpy()
    dt = np.median(np.diff(t))

    if pd.isna(dt) or dt <= 0:
        print(f"  Skipping {filename}: Invalid sampling interval (dt={dt}).")
        return None, ctx

    all_results = {}
    max_taus = []

    for col in cols:
        data = df[col].values
        taus, sigmas = _compute_allan_variance(data, dt)

        if len(taus) > 0:
            all_results[f"{col}_sigma"] = sigmas
            if len(max_taus) < len(taus):
                max_taus = taus

    if not all_results:
        return None, ctx

    res_df = pd.DataFrame({"tau_s": max_taus})
    for col_name, sigmas in all_results.items():
        res_df[col_name] = sigmas

    return res_df, ctx