from .core import Path,Context,pd


# =========================================================
# LOADER
# =========================================================

from pathlib import Path
import pandas as pd
import re
from datetime import datetime

pd.set_option("display.max_columns", None)

def parse_file_date(input_path:Path)->dict:
    filename = input_path.stem  # e.g., 'crash_data_20260313_165424__Phone003'
    
    # Matches exactly 8 digits, an underscore, and 6 digits anywhere in the name
    match = re.search(r"(\d{8})_(\d{6})", filename)
    
    if match:
        date_str = match.group(1)  # Captures the 8 digits (e.g., '20260313')
        time_str = match.group(2)  # Captures the 6 digits (e.g., '165424')
        
        try:
            date = {}
            dt = datetime.strptime(f"{date_str}_{time_str}", "%Y%m%d_%H%M%S")
            date["Date"] = dt.strftime("%Y-%m-%d")
            date["Time"] = dt.strftime("%H:%M:%S")
            return date
        except ValueError:
            return {}
    return {}
    


def load_csv(input_path: Path, context: Context):
    # Initialize the metadata dictionary
    metadata = {}

    
    # Read the file line by line to extract comments/metadata
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("#"):
                # Strip the '#' and whitespace, then try to split by the first ':'
                clean_line = line.lstrip("#").strip()
                if ":" in clean_line:
                    key, val = clean_line.split(":", 1)
                    metadata[key.strip()] = val.strip()
            else:
                # Stop reading once we hit the actual data rows to save time
                break

    if "Date" not in metadata:
        metadata.update(parse_file_date(input_path=input_path))
    if "log_row" in context:
        if "global_time" in context["log_row"]:
            global_time = context["log_row"]["global_time"]
            global_time = pd.to_datetime(global_time)

            # pandas.Timestamp already supports .date() and .time()
            context["global_time"] = global_time

            metadata["Date"] = global_time.date()
            metadata["Time"] = global_time.time()


    # Load the dataframe using the 'c' engine as before
    df = pd.read_csv(
        input_path,
        comment="#",
        engine="c",
    )

    # Update context
    context["input_path"] = input_path
    if "metadata" in context:
        context["metadata"].update(metadata)
    else:
        context["metadata"] = metadata
    
    return df, context

def load_excel(input_path: Path, context: Context):
    df = pd.read_excel(input_path)
    context["input_path"] = input_path
    return df, context



# =========================================================
# FRAMER LOADER
# =========================================================

def load_phone_drop_with_ref(input_path: Path, context: dict) -> tuple[pd.DataFrame, dict]:
    context["metadata"]={}


    if not input_path.exists():
        context["skip"] = True
        context["skip_reason"] = "input_doesnt_exist"
        return None, context
    
    ref_path = context["ref_path"]
    if not ref_path.exists():
        context["skip"] = True
        context["skip_reason"] = "ref_doesnt_exist"
        return None, context
    
    if input_path.suffix == ".csv":
        df,context = load_csv(input_path=input_path,context=context)
    elif  input_path.suffix == ".xlsx":
        df = pd.read_excel(input_path)
    else:
        context["skip"] = True
        context["skip_reason"] = f"input extension not recognised {input_path.suffix}"
        return None, context

    ref_df,ref_ctx = load_csv(input_path=ref_path,context={})
    context["metadata"].update(ref_ctx["metadata"])

    context["ref_df"] = ref_df
    context["ref_path"] = ref_path

    return df, context

# =========================================================
# PARSING SAVERS
# =========================================================


def save_single_csv(df: pd.DataFrame, context: Context) -> list[Path]:
    input_path: Path = context["input_path"]
    metadata: dict = context.get("metadata", {})  # Use .get() in case metadata isn't present

    if "output_path" in context:
        output_path:Path = context["output_path"]
    else:
        output_dir: Path = context["output_dir"]
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{input_path.stem}.csv"


    # 1. Open the file in write mode ('w') to write the metadata headers
    with open(output_path, "w", encoding="utf-8") as f:
        for key, value in metadata.items():
            f.write(f"# {key}: {value}\n")
            
    # 2. Open the file in append mode ('a') for pandas to dump the dataframe
    with open(output_path, "a", encoding="utf-8", newline="") as f:
        df.to_csv(f, index=False)

    context["output"] = output_path
    return [output_path]



def save_split_by_triggered(df: pd.DataFrame, context: Context) -> list[Path]:

    output_dir: Path = context["output_dir"]
    input_path: Path = context["input_path"]

    output_dir.mkdir(parents=True, exist_ok=True)

    outputs: list[Path] = []

    for trig in sorted(df["triggered"].unique()):

        sub = df[df["triggered"] == trig].reset_index(drop=True)

        out = output_dir / f"{input_path.stem}_trigger{trig}.csv"
        context["output_path"] = out
        save_single_csv(sub,context)
        outputs.append(out)

    return outputs

# =========================================================
# FRAMING SAVERS
# =========================================================
from .naming import parse_raw_filename

def save_stationary(
    df: pd.DataFrame,
    context: Context,
    both: bool = False
) -> list[Path]:

    file: Path = context["input_path"]
    output_dir: Path = context["output_dir"]

    output_dir.mkdir(parents=True, exist_ok=True)

    sensor, date, time, phone_id, _ = parse_raw_filename(file.stem)

    if not all([date, time, phone_id]):
        raise ValueError(f"Invalid filename format: {file.stem}")

    if not both:
        if not sensor:
            raise ValueError(f"Missing sensor in filename: {file.stem}")
        out_name = f"{sensor}_stationary_{date}_{time}_{phone_id}.csv"
    else:
        out_name = f"both_stationary_{date}_{time}_{phone_id}.csv"

    out_path = output_dir / out_name
    context["output_path"] = out_path

    return save_single_csv(df, context)


def save_headform_stationary(df: pd.DataFrame, context: Context) -> list[Path]:
    output_dir: Path = context["output_dir"]
    output_dir.mkdir(parents=True, exist_ok=True)
    
    metadata = context.get("metadata", {})
    # Expecting 'Test Date' like '15/05/2026' and 'Test Time' like '14:38:07'
    date = metadata.get("Test Date", "unknown").replace("/", "")
    time = metadata.get("Test Time", "unknown").replace(":", "")
    phone_id = metadata.get("phone_id", "Headform")
    
    out_name = f"both_stationary_{date}_{time}_{phone_id}.csv"
    out_path = output_dir / out_name
    context["output_path"] = out_path
    
    return save_single_csv(df, context)

def save_allan_variance(df: pd.DataFrame, context: Context) -> list[Path]:
    """
    Saves the Allan variance results with an '_allan.csv' suffix.
    """
    if df is None:
        return []
        
    output_dir: Path = context["output_dir"]
    input_path: Path = context["input_path"]
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    out_name = f"{input_path.stem}_allan.csv"
    out_path = output_dir / out_name

    context["output_path"]=out_path

    return save_single_csv(df,context)

def save_psd(df: pd.DataFrame, context: Context) -> list[Path]:
    """
    Saves the PSD results with a '_psd.csv' suffix.
    """
    if df is None:
        return []
        
    output_dir: Path = context["output_dir"]
    input_path: Path = context["input_path"]
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    out_name = f"{input_path.stem}_psd.csv"
    out_path = output_dir / out_name

    context["output_path"]=out_path

    return save_single_csv(df,context)

def null_saver(df: pd.DataFrame, context: Context) -> list[Path]:
    """
    Does not save anything to disk. Used when we only care about the context results.
    """
    return []


# =========================================================
# Logging SAVERS
# =========================================================


def load_headform_csv(input_path: Path, context: Context):
    metadata = {}
    data_start_row = None

    with open(input_path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if "Data Starts Here" in line:
                data_start_row = i
                break
            if "," in line:
                parts = line.split(",")
                if len(parts) >= 2:
                    key = parts[0].strip()
                    val = parts[1].strip()
                    if key:
                        metadata[key] = val

    if data_start_row is None:
        raise ValueError(f"Could not find 'Data Starts Here' in {input_path}")

    # Read the header row separately to get cleaned column names
    df_headers = pd.read_csv(input_path, skiprows=data_start_row + 1, nrows=0)
    col_names = df_headers.columns.str.strip().tolist()

    # Read data rows
    df = pd.read_csv(
        input_path, 
        skiprows=data_start_row + 2, 
        header=None, 
        names=col_names, 
        engine='c'
    )

    # Clean up any trailing empty columns
    df = df.loc[:, ~df.columns.str.contains("^Unnamed", case=False, na=False)]
    df = df.dropna(axis=1, how="all")

    context["input_path"] = input_path
    context["metadata"] = metadata
    context["metadata"]["phone_id"] = "Headform"

    return df, context

def load_raw_log_csv(path: Path, ctx: Context):
    # IMPORTANT: logs are messy, so we avoid strict parsing
    df = pd.read_csv(path, comment="#", dtype=str)

    ctx["input_path"] = path
    return df, ctx



# =========================================================
# Plotly SAVERS
# =========================================================

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

def get_html_path(context:Context):
    input_path: Path = context["input_path"]

    if "output_path" in context:
        output_path:Path = context["output_path"]
    else:
        output_dir: Path = context["output_dir"]
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{input_path.stem}.html"
        
    return output_path

def save_interactive_plot(df:pd.DataFrame, ctx:Context):
    time = df["Time (s)"].to_numpy()
    dt = np.diff(time)
    t_mid = time[1:]

    # Create subplots (2 rows, 1 column)
    fig = make_subplots(rows=2, cols=1, subplot_titles=(
        "Sampling Interval (Δt) Density Spectrum", 
        "Sampling Interval Distribution (log y)"
    ))

    # Top Plot: 2D Histogram
    fig.add_trace(
        go.Histogram2d(
            x=t_mid, y=dt, 
            nbinsx=120, nbinsy=120, 
            colorscale='Jet',
            cmin=1,
            colorbar=dict(title="Density Count", len=0.45, y=0.75)
        ),
        row=1, col=1
    )

    # Bottom Plot: 1D Histogram (Log Scale)
    fig.add_trace(
        go.Histogram(
            x=dt, 
            nbinsx=100,
            name="Δt distribution"
        ),
        row=2, col=1
    )
    
    # Configure layouts and log scale for the bottom plot
    fig.update_yaxes(type="log", row=2, col=1)
    fig.update_xaxes(title_text="Time (s)", row=1, col=1)
    fig.update_yaxes(title_text="Δt (ns)", row=1, col=1)
    fig.update_xaxes(title_text="Δt (ns)", row=2, col=1)
    fig.update_yaxes(title_text="Count (log)", row=2, col=1)
    
    fig.update_layout(height=800, width=1000, showlegend=False)

    output_path = get_html_path(ctx)

    fig.write_html(output_path, include_plotlyjs='cdn')
    return [output_path]



def save_doubled_interactive_plot(df:pd.DataFrame, ctx:Context):
    accel_cols = ["LinAccelX (m/s)", "LinAccelY (m/s)", "LinAccelZ (m/s)"]
    gyro_cols  = ["RotVelX (rad/s)", "RotVelY (rad/s)", "RotVelZ (rad/s)"]

    time = df["Time (s)"].to_numpy()

    # 1. Helper to keep only rows where sensor values change
    def change_times(cols):
        vals = df[cols].to_numpy(dtype=np.float32)
        keep = np.ones(len(df), dtype=bool)
        same = np.all(vals[1:] == vals[:-1], axis=1)
        keep[1:] = ~same
        return time[keep]

    accel_times = change_times(accel_cols)
    gyro_times  = change_times(gyro_cols)

    # 2. Compute Δt
    def inst_dt(t):
        dt = np.diff(t)
        t_mid = t[1:]
        return dt, t_mid

    accel_dt, accel_dt_t = inst_dt(accel_times)
    gyro_dt, gyro_dt_t   = inst_dt(gyro_times)

    # 3. Peak markers (scalar timestamp values)
    t_max_accelZ = df.loc[df["accelZ_g"].idxmax(), "Time (s)"]
    t_max_gyroX  = df.loc[df["gyroX_dps"].idxmax(), "Time (s)"]

    # 4. Initialize a 2x2 grid layout
    fig = make_subplots(
        rows=2, cols=2, 
        subplot_titles=(
            "Accel Δt Density Spectrum", "Gyro Δt Density Spectrum",
            "Accel Δt distribution (log y)", "Gyro Δt distribution (log y)"
        ),
        vertical_spacing=0.12,
        horizontal_spacing=0.12
    )

    # =================================================
    # TOP ROW: 2D Density Histograms
    # =================================================
    # Accel 2D Histogram (Row 1, Col 1)
    fig.add_trace(
        go.Histogram2d(
            x=accel_dt_t, y=accel_dt, nbinsx=120, nbinsy=120, 
            colorscale='Jet', cmin=1, name='Accel Density',
            colorbar=dict(title="Accel Count", len=0.4, y=0.8, x=0.42, yanchor="center")
        ),
        row=1, col=1
    )

    # Gyro 2D Histogram (Row 1, Col 2)
    fig.add_trace(
        go.Histogram2d(
            x=gyro_dt_t, y=gyro_dt, nbinsx=120, nbinsy=120, 
            colorscale='Jet', cmin=1, name='Gyro Density',
            colorbar=dict(title="Gyro Count", len=0.4, y=0.8, x=1.02, yanchor="center")
        ),
        row=1, col=2
    )

    # =================================================
    # BOTTOM ROW: 1D Δt distributions (Log)
    # =================================================
    # Accel 1D (Row 2, Col 1)
    fig.add_trace(
        go.Histogram(x=accel_dt, nbinsx=100, marker=dict(color='#1f77b4'), name="Accel Count"),
        row=2, col=1
    )

    # Gyro 1D (Row 2, Col 2)
    fig.add_trace(
        go.Histogram(x=gyro_dt, nbinsx=100, marker=dict(color='#ff7f0e'), name="Gyro Count"),
        row=2, col=2
    )

    # =================================================
    # Add Vertical Peak Lines to Top Plots
    # =================================================
    # Accel Max Line
    fig.add_vline(x=t_max_accelZ, line_width=1.5, line_dash="dash", line_color="black", row=1, col=1)
    # Replicate original legend entry behavior by adding an empty proxy trace
    fig.add_trace(go.Scatter(x=[None], y=[None], mode='lines', line=dict(color='black', dash='dash'), name='max accelZ'), row=1, col=1)

    # Gyro Max Line
    fig.add_vline(x=t_max_gyroX, line_width=1.5, line_dash="dash", line_color="black", row=1, col=2)
    fig.add_trace(go.Scatter(x=[None], y=[None], mode='lines', line=dict(color='black', dash='dash'), name='max gyroX'), row=1, col=2)

    # =================================================
    # Styling and Axis Configuration
    # =================================================
    # Row 1 Labels
    fig.update_xaxes(title_text="Time (ns)", row=1, col=1)
    fig.update_yaxes(title_text="Δt (ns)", row=1, col=1)
    fig.update_xaxes(title_text="Time (ns)", row=1, col=2)
    fig.update_yaxes(title_text="Δt (ns)", row=1, col=2)

    # Row 2 Labels + Enable Log Scales
    fig.update_xaxes(title_text="Δt (ns)", row=2, col=1)
    fig.update_yaxes(title_text="Count (log)", type="log", row=2, col=1)
    fig.update_xaxes(title_text="Δt (ns)", row=2, col=2)
    fig.update_yaxes(title_text="Count (log)", type="log", row=2, col=2)

    # Global Layout
    fig.update_layout(
        title_text=f"Sensor Refresh Rate Diagnostics: {ctx.get('input_path', '')}",
        title_x=0.5,
        height=950, 
        width=1400, 
        showlegend=True, # Set to True to display the custom line labels
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        template="plotly_white"
    )

    output_path = get_html_path(ctx)

    fig.write_html(output_path, include_plotlyjs='cdn')
    return [output_path]