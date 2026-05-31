import pandas as pd
import numpy as np
import re
from pathlib import Path
from scipy.interpolate import interp1d
from ..core import Context

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
    
    # Metadata columns we want to grab the most common string value (the mode)
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
