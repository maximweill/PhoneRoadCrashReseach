import pandas as pd
import numpy as np
import re
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
    context: Context,
) -> tuple[pd.DataFrame, Context]:

    if "time_ns" not in df.columns:
        if "sensor_time_ns" in df.columns:
            df = df.rename(columns={"sensor_time_ns": "time_ns"})
        else:
            raise ValueError("Missing time column")

    return df, context


def ensure_sensor_columns(
    df: pd.DataFrame,
    context: Context,
) -> tuple[pd.DataFrame, Context]:

    cols = [
        "accelX_g", "accelY_g", "accelZ_g","accelMag_g",
        "gyroX_dps", "gyroY_dps", "gyroZ_dps","gyroMag_dps",
    ]

    for c in cols:
        if c not in df.columns:
            df[c] = 0.0

    return df, context


def sort_by_time(
    df: pd.DataFrame,
    context: Context,
) -> tuple[pd.DataFrame, Context]:

    if not df["time_ns"].is_monotonic_increasing:
        df = df.sort_values("time_ns").reset_index(drop=True)

    return df, context


def deduplicate_DEPRECATED(
    df: pd.DataFrame,
    context: Context,
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

    context["removed_rows"] = removed
    context["initial_rows"] = len(keep)
    context["final_rows"] = len(df)

    return df, context

def accelerometer_based_timestamps(
    df: pd.DataFrame,
    context: Context,
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

    context["acc_removed_rows"] = removed
    context["acc_initial_rows"] = len(keep)
    context["acc_final_rows"] = len(df)

    return df, context

def deduplicate(
    df: pd.DataFrame,
    context: Context,
) -> tuple[pd.DataFrame, Context]:

    keep = ~df["time_ns"].duplicated(keep="first")

    removed = int((~keep).sum())

    df = df.loc[keep].reset_index(drop=True)
    
    context["dedup_removed_rows"] = removed
    context["dedup_initial_rows"] = len(keep)
    context["dedup_final_rows"] = len(df)

    return df, context

def convert_units(
    df: pd.DataFrame,
    context: Context,
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

    return df, context


# =========================================================
# FRAMING TRANSFORMS
# =========================================================
from scipy import signal

def ref_timestamps_matching(df: pd.DataFrame, context: Context):
    ref_time = context["ref_df"]["Time (s)"]

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

    context["len_ratio"] = len(df)/len(df_new)

    return df_new, context

def compute_lag(df: pd.DataFrame, context: Context):
    ref_df = context["ref_df"]

    sig_col = context.get("signal_col", "RotVelX (rad/s)")
    ref_sig_col = context.get("ref_signal_col", "RotVelX (rad/s)")

    sig_p = np.nan_to_num(df[sig_col].to_numpy())
    sig_r = np.nan_to_num(ref_df[ref_sig_col].to_numpy())

    sig_p = (sig_p - sig_p.mean()) / (sig_p.std() + 1e-9)
    sig_r = (sig_r - sig_r.mean()) / (sig_r.std() + 1e-9)

    corr = signal.correlate(sig_p, sig_r, mode="full", method="fft")
    lags = signal.correlation_lags(len(sig_p), len(sig_r), mode="full")

    context["lag_idx"] = int(lags[np.argmax(corr)])
    return df, context

def align_to_reference_deprecated(
    df: pd.DataFrame,
    context: Context,
) -> tuple[pd.DataFrame, Context]:

    lag = int(context.get("lag_idx", 0))
    ref_df = context["ref_df"]

    # clamp lag to valid range (prevents crashes on edge cases)
    lag = max(0, min(lag, len(df) - 1))

    t0_ref = ref_df["Time (s)"].iloc[0]
    t_at_lag = df["Time (s)"].iloc[lag]

    offset = t0_ref - t_at_lag
    context["offset"] = offset

    # align window to reference length
    end = min(lag + len(ref_df), len(df))
    df = df.iloc[lag:end].copy()

    df["Time (s)"] = df["Time (s)"] + offset

    return df, context

def align_to_reference(
    df: pd.DataFrame,
    context: Context,
) -> tuple[pd.DataFrame, Context]:

    lag = int(context.get("lag_idx", 0))
    ref_df = context["ref_df"]

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

    context["offset"] = offset

    return df_aligned, context

def trim_stationary(
    df: pd.DataFrame,
    context: Context,
) -> tuple[pd.DataFrame, Context]:

    threshold = context.get("impact_threshold", 11)

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

    start_time += context.get("buffer_after", 600)
    end_time -= context.get("buffer_before", 60)

    df = df[(df["Time (s)"] > start_time) & (df["Time (s)"] < end_time)].copy()

    if len(df) > 0:
        df["Time (s)"] -= df["Time (s)"].iloc[0]

    return df, context


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
    
    # Extract phone id from filename if not already present
    if "phone_id" not in ctx:
        match = re.search(r"(Phone\d+)", ctx["input_path"].name)
        ctx["phone_id"] = match.group(1) if match else "Unknown"

    # Define standard metrics we want to keep
    characteristics_keys = [
        "phone_id", "fs_mean", "fs_median", "fs_iqr", 
        "battery_temp_c_mean", "battery_temp_c_median", "battery_temp_c_iqr", "initial_magX_uT", "initial_magY_uT", "initial_magZ_uT"
    ]
    
    # Create the row and also clean the context so run_directory produces a clean log
    summary_row = {}
    
    # Identify keys to keep in ctx for the final log_df
    keys_to_keep = set()
    
    # Always keep filename
    summary_row["filename"] = ctx["input_path"].name

    for k, v in ctx.items():
        if k.startswith("max_") or k in characteristics_keys:
            summary_row[k] = v
            keys_to_keep.add(k)
        elif not k.startswith("_") and k not in ["output_dir", "input_path", "accel", "gyro", "rot_acc"] and isinstance(v, (int, float, str)):
            summary_row[k] = v
            keys_to_keep.add(k)

    # Prune ctx of large objects or irrelevant data to keep run_directory's output clean
    all_keys = list(ctx.keys())
    for k in all_keys:
        if k not in keys_to_keep and not k.startswith("_") and k != "input_path":
            # We don't delete input_path as it might be needed by the runner logic internally
            # but we can set to None or ignore
            pass
            
    summary_df = pd.DataFrame([summary_row])
    return summary_df, ctx

def aggregate_characteristics_by_phone(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    """
    Groups the collected characteristics by Phone ID and applies aggregation rules.
    """
    print("Aggregating characteristics by Phone ID...")
    
    if "phone_id" not in df.columns:
        print("NO PHONE ID in individual char")
        return df, ctx
        
    avg_metrics = ["fs_mean", "fs_median", "fs_iqr", 
                   "battery_temp_c_mean", "battery_temp_c_median", "battery_temp_c_iqr",
                   "initial_magX_uT", "initial_magY_uT", "initial_magZ_uT"]
    
    # Metadata columns we want to mode
    metadata_cols = ["Device", "Accelerometer", "Gyroscope", "Magnetometer"]
    
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
            agg_rules[col] = lambda x: x.dropna().mode().iloc[0] if not x.dropna().mode().empty else np.nan

    df_aggregated = df.groupby("phone_id").agg(agg_rules).reset_index()
    
    # Rounding for readability
    numeric_cols = df_aggregated.select_dtypes(include=[np.number]).columns
    df_aggregated[numeric_cols] = df_aggregated[numeric_cols].round(3)

    return df_aggregated, ctx

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
# ALLAN VARIANCE TRANSFORMS
# =========================================================

def calculate_allan_variance_transform(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    """
    Computes Allan variance for relevant columns in the dataframe.
    """
    filename = ctx["input_path"].name

    # Determine columns to analyze based on file prefix
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