import pandas as pd
import numpy as np
import re
from pathlib import Path
from ..core import Context
from .base import G, DEG2RAD
import matplotlib.pyplot as plt

# =========================================================
# PARSING TRANSFORMS
# =========================================================

def normalize_column_names(df: pd.DataFrame, ctx: Context):
    # Conservative cleaning: strip and remove only truly problematic chars
    # Keeps spaces, parens, and slashes for unit consistency
    df.columns = (
        df.columns
        .str.strip()
        .str.replace(r"[^a-zA-Z0-9 ()//_]", "", regex=True)
    )
    return df, ctx

def normalize_time_column(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    if "Time (s)" in df.columns:
        return df, ctx

    # Phyphox raw output
    if "sensor_time_ns" in df.columns:
        df = df.rename(columns={"sensor_time_ns": "time_ns"})

    if "time_ns" in df.columns:
        df["Time (s)"] = df["time_ns"].astype(np.float64) / 1e9
        df = df.drop(columns=["time_ns"])
    elif "time" in df.columns:
        df = df.rename(columns={"time": "Time (s)"})
    
    if "Time (s)" not in df.columns:
        raise ValueError(f"Missing time column. Found columns: {df.columns.tolist()}")

    return df, ctx

def rename_ind_colums(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:
    rename_map = {
        "accelX":"LinAccX (m/s2)",
        "accelY":"LinAccY (m/s2)",
        "accelZ":"LinAccZ (m/s2)",

        "gyroX":"RotVelX (rad/s)",
        "gyroY":"RotVelY (rad/s)",
        "gyroZ":"RotVelZ (rad/s)",
        }

    df = df.rename(columns=rename_map)
    
    for _,parsed in rename_map.items():
        if parsed not in df.columns:
            df[parsed] = np.float16(0)
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
            df[c] = np.float16(0)

    return df, ctx

def normalize_headform_columns(df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, Context]:
    # Take inspiration from DEPRECATED_headform_parser.py
    rename_map = {
        'Chan 0:6DX0855-AV1': 'gyroZ_dps',
        'Chan 1:6DX0855-AV2': 'gyroY_dps',
        'Chan 2:6DX0855-AV3': 'gyroX_dps',
        'Chan 3:6DX0855-AC1': 'accelZ_g',
        'Chan 4:6DX0855-AC2': 'accelY_g',
        'Chan 5:6DX0855-AC3': 'accelX_g',
        'Time': 'Time (s)'
    }
    
    # If the exact names are not found, try index-based as fallback
    if not any(col in df.columns for col in rename_map.keys() if col != 'Time'):
        mapping = {
            0: "Time (s)",
            1: "gyroZ_dps",
            2: "gyroY_dps",
            3: "gyroX_dps",
            4: "accelZ_g",
            5: "accelY_g",
            6: "accelX_g"
        }
        new_cols = []
        for i in range(len(df.columns)):
            if i in mapping:
                new_cols.append(mapping[i])
            else:
                new_cols.append(df.columns[i])
        df.columns = new_cols
    else:
        df = df.rename(columns=rename_map)
    
    # Ensure all sensor columns are numeric
    sensor_cols = [
        'gyroZ_dps', 'gyroY_dps', 'gyroX_dps', 
        'accelZ_g', 'accelY_g', 'accelX_g', 'Time (s)'
    ]
    for col in sensor_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    return df, ctx

def sort_by_time(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    if not df["Time (s)"].is_monotonic_increasing:
        df = df.sort_values("Time (s)").reset_index(drop=True)

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
    ctx["acc_ratio"] = f"{len(df)/len(keep):0.1f}"

    return df, ctx



def deduplicate(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    keep = ~df["Time (s)"].duplicated(keep="first")

    removed = int((~keep).sum())

    df = df.loc[keep].reset_index(drop=True)
    
    ctx["dedup_removed_rows"] = removed
    ctx["dedup_initial_rows"] = len(keep)
    ctx["dedup_final_rows"] = len(df)
    ctx["dedup_ratio"] = f"{len(df)/len(keep):0.1f}"

    return df, ctx

def convert_units(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    t = df["Time (s)"].to_numpy(dtype=np.float64)

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

    preserve = ["trigger", "axis", "triggered", "batt_temp_c"]
    extra_cols = {c: df[c].to_numpy() for c in preserve if c in df.columns}

    df = pd.DataFrame({
        "Time (s)": t,

        "LinAccX (m/s2)": accel_vec[:, 0],
        "LinAccY (m/s2)": accel_vec[:, 1],
        "LinAccZ (m/s2)": accel_vec[:, 2],

        "RotVelX (rad/s)": gyro_vec[:, 0],
        "RotVelY (rad/s)": gyro_vec[:, 1],
        "RotVelZ (rad/s)": gyro_vec[:, 2],

        **extra_cols,
    })

    return df, ctx

def add_rotational_acceleration(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    t = df["Time (s)"].to_numpy(dtype=np.float64)

    gyro_vec = df[
        ["RotVelX (rad/s)", "RotVelY (rad/s)", "RotVelZ (rad/s)"]
    ].to_numpy(dtype=np.float32)

    rot_acc_vec = np.gradient(gyro_vec, t, axis=0)

    df["RotAccX (rad/s2)"] = rot_acc_vec[:, 0]
    df["RotAccY (rad/s2)"] = rot_acc_vec[:, 1]
    df["RotAccZ (rad/s2)"] = rot_acc_vec[:, 2]

    return df, ctx

def add_magnitudes(
    df: pd.DataFrame,
    ctx: Context,
) -> tuple[pd.DataFrame, Context]:

    df["LinAccRes (m/s2)"] = np.linalg.norm(
        df[["LinAccX (m/s2)", "LinAccY (m/s2)", "LinAccZ (m/s2)"]],
        axis=1,
    )

    df["RotVelRes (rad/s)"] = np.linalg.norm(
        df[["RotVelX (rad/s)", "RotVelY (rad/s)", "RotVelZ (rad/s)"]],
        axis=1,
    )

    df["RotAccRes (rad/s2)"] = np.linalg.norm(
        df[["RotAccX (rad/s2)", "RotAccY (rad/s2)", "RotAccZ (rad/s2)"]],
        axis=1,
    )

    return df, ctx


# def sensor_refresh_rate_diagnostics(df: pd.DataFrame, ctx: dict):
#     accel_cols = ["accelX_g", "accelY_g", "accelZ_g"]
#     gyro_cols  = ["gyroX_dps", "gyroY_dps", "gyroZ_dps"]

#     time_col = "sensor_time_ns" if "sensor_time_ns" in df.columns else "time_ns"
#     time = df[time_col].to_numpy()

#     # -------------------------------------------------
#     # helper: keep only rows where sensor values change
#     # -------------------------------------------------
#     def change_times(cols):
#         vals = df[cols].to_numpy(dtype=np.float32)

#         keep = np.ones(len(df), dtype=bool)
#         same = np.all(vals[1:] == vals[:-1], axis=1)
#         keep[1:] = ~same

#         return time[keep]

#     accel_times = change_times(accel_cols)
#     gyro_times  = change_times(gyro_cols)

#     # -------------------------------------------------
#     # compute Δt
#     # -------------------------------------------------
#     def inst_dt(t):
#         dt = np.diff(t)
#         t_mid = t[1:]
#         return dt, t_mid

#     accel_dt, accel_dt_t = inst_dt(accel_times)
#     gyro_dt, gyro_dt_t   = inst_dt(gyro_times)

#     # -------------------------------------------------
#     # peak markers (from original df)
#     # -------------------------------------------------
#     t_max_accelZ = df.loc[df["accelZ_g"].idxmax(), time_col]
#     t_max_gyroX  = df.loc[df["gyroX_dps"].idxmax(), time_col]

#     # -------------------------------------------------
#     # plotting
#     # -------------------------------------------------
#     fig, axes = plt.subplots(2, 2, figsize=(14, 9))

#     # =================================================
#     # TOP ROW: Modified to 2D Density Histogram (Spectrometer style)
#     # =================================================
#     # bins=(100, 100) sets the spectrometer resolution grid
#     # cmap='jet' provides the blue->green->yellow->red rainbow spectrum
    
#     # Accel Plot
#     im0 = axes[0, 0].hist2d(accel_dt_t, accel_dt, bins=(120, 120), cmap='jet', cmin=1)
#     axes[0, 0].axvline(t_max_accelZ, color="black", linestyle="--", label="max accelZ")
#     axes[0, 0].set_title("Accel Δt Density Spectrum")
#     axes[0, 0].set_xlabel("Time (ns)")
#     axes[0, 0].set_ylabel("Δt (ns)")
#     axes[0, 0].legend()
#     fig.colorbar(im0[3], ax=axes[0, 0], label='Density Count')

#     # Gyro Plot
#     im1 = axes[0, 1].hist2d(gyro_dt_t, gyro_dt, bins=(120, 120), cmap='jet', cmin=1)
#     axes[0, 1].axvline(t_max_gyroX, color="black", linestyle="--", label="max gyroX")
#     axes[0, 1].set_title("Gyro Δt Density Spectrum")
#     axes[0, 1].set_xlabel("Time (ns)")
#     axes[0, 1].set_ylabel("Δt (ns)")
#     axes[0, 1].legend()
#     fig.colorbar(im1[3], ax=axes[0, 1], label='Density Count')

#     # =========================
#     # BOTTOM ROW: Δt distributions (log scale)
#     # =========================
#     axes[1, 0].hist(accel_dt, bins=100, log=True)
#     axes[1, 0].set_title("Accel Δt distribution (log y)")
#     axes[1, 0].set_xlabel("Δt (ns)")
#     axes[1, 0].set_ylabel("Count (log)")

#     axes[1, 1].hist(gyro_dt, bins=100, log=True)
#     axes[1, 1].set_title("Gyro Δt distribution (log y)")
#     axes[1, 1].set_xlabel("Δt (ns)")
#     axes[1, 1].set_ylabel("Count (log)")

#     fig.suptitle(ctx["input_path"], fontsize=12)

#     plt.tight_layout()
#     plt.show()

#     return df, ctx


# def timestamp_diagnostics(df: pd.DataFrame, ctx: dict, time_col: str = "time_ns"):
#     # 1. Extract timestamps
#     time = df[time_col].to_numpy()

#     # 2. Compute Δt
#     dt = np.diff(time)
#     t_mid = time[1:]

#     # 3. Plotting
#     fig, axes = plt.subplots(2, 1, figsize=(11, 9))

#     # =================================================
#     # TOP: Modified to 2D Density Histogram (Spectrometer style)
#     # =================================================
#     im2 = axes[0].hist2d(t_mid, dt, bins=(120, 120), cmap='jet', cmin=1)
#     axes[0].set_title("Sampling Interval (Δt) Density Spectrum")
#     axes[0].set_xlabel(f"Time ({time_col})")
#     axes[0].set_ylabel("Δt (ns)")
#     fig.colorbar(im2[3], ax=axes[0], label='Density Count')

#     # =========================
#     # BOTTOM: Δt distribution (log scale)
#     # =========================
#     axes[1].hist(dt, bins=100, log=True)
#     axes[1].set_title("Sampling Interval Distribution (log y)")
#     axes[1].set_xlabel("Δt (ns)")
#     axes[1].set_ylabel("Count (log)")

#     fig.suptitle(ctx["input_path"], fontsize=12)

#     plt.tight_layout()
#     plt.show()

#     return df, ctx