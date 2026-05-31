# =============================================================================
# HEAD CG TO PHONE IMU TRANSFORMATION SCRIPT
# =============================================================================
# Reads individual headform impact .csv files, filters signals, derives
# rotational acceleration, transforms from head CG to 4 phone IMU locations,
# and outputs 4 individual .xlsx files per impact.
#
# INPUT FILE FORMAT:
#   - ~20 metadata rows before data
#   - Row with "Data Starts Here" marks end of metadata
#   - Data columns: Time, AV-Z, AV-Y, AV-X, AC-Z, AC-Y, AC-X
#   - Units: deg/sec (rotational velocity), g (linear acceleration)
#
# OUTPUT:
#   - 4 .xlsx files per impact, labelled with phone number 001-004
#   - Columns: Time, LinAccX, LinAccY, LinAccZ, LinAccRes,
#              RotVelX, RotVelY, RotVelZ, RotVelRes,
#              RotAccX, RotAccY, RotAccZ, RotAccRes
#   - Units: m/s² (linear acceleration), rad/s (rotational velocity),
#            rad/s² (rotational acceleration)
#
# KNOWN LIMITATIONS:
#   - Gravity is NOT removed from the linear acceleration signal.
#     The rigid body transformation is applied to the raw measured
#     acceleration which includes the gravitational component.
#     Users should be aware of this when interpreting results,
#     particularly for slow or quasi-static motions where gravity
#     is significant relative to impact acceleration.
#
#   - The combined CG of the headform and mounted phones may differ
#     slightly from the headform CG alone. This offset has not been
#     corrected. Users should quantify this offset and assess whether
#     it is significant relative to the displacement vectors.
# =============================================================================
"""
README
# Head CG to Phone IMU Transformation Script

## Overview
This script reads headform impact test data recorded by a 6DX inertial
measurement unit (IMU) positioned at the head centre of gravity (CG),
and transforms the measured kinematics to four phone IMU locations
mounted on the headform. Output files represent the predicted signals
that each phone IMU would measure at its respective location.

## Purpose
To enable comparison (Use Case A: validation) between headform-derived
predicted kinematics at phone IMU locations against direct phone IMU
measurements recorded during the same impact events. Phones were
physically present and mounted on the headform during all tests.

---

## Input Data Format
- File format  : .csv
- Source       : 6DX IMU data acquisition system
- Structure    : ~20 metadata rows, followed by 'Data Starts Here'
                 marker, followed by a column header row, then numeric
                 data
- Columns      : Time | AV-Z | AV-Y | AV-X | AC-Z | AC-Y | AC-X
- Units        : deg/s (rotational velocity), g (linear acceleration)
- Sample rate  : 20,000 Hz
- Data points  : 2001 pre-trigger + 10001 post-trigger = 12,002 total

---

## Processing Pipeline

### 1. Unit Conversion and Sign Correction
- Rotational velocity : deg/s -> rad/s  (x pi/180)
- Linear acceleration : g    -> m/s²   (x 9.81)
- Per-axis sign corrections applied via SIGN_CORRECTIONS parameter
- !! Sign corrections must be validated against a known impact
     direction before processing the full dataset !!

### 2. Outlier Removal
- Hampel filter applied to all channels
- Window half-size : k = 3 samples
- Threshold        : 2 scaled MAD standard deviations
- Implemented using pandas rolling functions for efficiency at
  20,000 Hz sample rate

### 3. Low-Pass Filtering (SAE J211 CFC 1000)
- Linear acceleration filtered using a 4th-order zero-phase Butterworth
  filter at 1650 Hz cutoff (SAE J211 CFC 1000)
- Rotational velocity filtered using a 4th-order zero-phase Butterworth
  filter at 300 Hz cutoff (SAE J211 CFC 180)
- Implemented via scipy filtfilt (zero-phase, no time delay)

### 4. Rotational Acceleration Derivation
- Derived from filtered rotational velocity using a five-point
  stencil finite difference approximation (vectorised)
- Interior points (4th order accurate):
    dω/dt = (−ω[i+2] + 8ω[i+1] − 8ω[i−1] + ω[i−2]) / 12h
- Boundary points use forward/backward finite differences
  (1st and 2nd order accurate respectively)
- Derived rotational acceleration not subsequently filtered as this
  would stack filtering on the rotational component
### 5. Rigid Body Transformation (Head CG -> Phone Location)
- Transforms linear acceleration from head CG to each phone IMU
  location using the rigid body equation:
    a_phone = a_CG + alpha x r + omega x (omega x r)
  where r is the displacement vector from head CG to phone IMU
  (metres), expressed in the head CG coordinate frame
- Fully vectorised using numpy broadcasting for efficiency
- Angular velocity (omega) and angular acceleration (alpha) are
  taken from the filtered/derived head CG signals

### 6. Coordinate Frame Rotation (Head Frame -> Phone Frame)
- All signals (linear acceleration, rotational velocity, rotational
  acceleration) are rotated from the head CG coordinate frame into
  the phone IMU coordinate frame using rotation matrix R:
    phone_x = -head_y
    phone_y = -head_x
    phone_z =  head_z
- All four phones are assumed to share the same orientation relative
  to the headform

---

## Output Data Format
- File format : .xlsx (4 files per impact, one per phone)
- Naming      : {input_filename}_Transformed_Phone001.xlsx
                                               ...Phone004.xlsx
- Columns     : Time (s)
                LinAccX/Y/Z/Res  (m/s^2)
                RotVelX/Y/Z/Res  (rad/s)
                RotAccX/Y/Z/Res  (rad/s^2)

---

## User-Defined Parameters
All parameters are clearly marked at the top of the script:

| Parameter        | Description                                        |
|------------------|----------------------------------------------------|
| INPUT_FOLDER     | Path to folder containing input .csv files         |
| OUTPUT_FOLDER    | Path to folder for output .xlsx files              |
| FS               | Sample rate in Hz (default: 20000)                 |
| FC_LIN           | Linear acceleration filter cutoff Hz (default: 1000)|
| FC_ROT           | Rotational filter cutoff Hz (default: 1000)        |
| FILTER_ORDER     | Butterworth filter order (default: 4)              |
| SIGN_CORRECTIONS | Per-axis sign correction flags (+1 or -1)          |
| PHONE_VECTORS    | Displacement vectors head CG to phone IMU (metres) |
| R_HEAD_TO_PHONE  | Rotation matrix head frame to phone frame          |

---

## Assumptions
1.  The 6DX reference sensor is located at the head CG of the
    headform. Any offset between the physical sensor location and
    the true head CG is assumed negligible.

2.  The phones were physically present and mounted on the headform
    during all impact tests. The combined CG of the headform and
    mounted phones may differ from the headform CG alone. This
    offset has not been corrected. Users should calculate the
    combined CG using:
        r_CG_combined = (m_head*r_head + sum(m_phone*r_phone))
                        / (m_head + sum(m_phone))
    and assess whether the offset is significant relative to the
    displacement vectors.

3.  All phone IMUs are rigidly fixed to the headform. Any compliance
    in the phone mount is assumed negligible.

4.  All four phone IMUs share the same orientation relative to the
    headform coordinate frame (same rotation matrix R applied to
    all four phones).

5.  The sensor axes of the 6DX are assumed to be aligned with the
    headform coordinate frame. No additional calibration rotation
    is applied.

6.  GRAVITY IS NOT REMOVED. The linear acceleration signal retains
    its gravitational component throughout all processing and
    transformation steps. The rigid body transformation is therefore
    applied to the total measured acceleration (kinematic +
    gravitational). Users should be aware of this when interpreting
    results. For high-acceleration impact events this effect is
    typically small relative to peak impact accelerations, but may
    be non-negligible for lower-severity events or quasi-static
    motions. Future work should consider explicit gravity removal
    using the pre-trigger window if the headform orientation is
    consistent across tests.

7.  The five-point stencil boundary approximations are assumed to be
    sufficiently accurate given that impact events do not occur at
    the signal boundaries. The 2001 pre-trigger samples provide
    adequate padding.

8.  A 1650 Hz cutoff frequency (CFC 1000, SAE J211) is applied to linear
    channels and CFC 180 (300Hz) for rotational channels.

9.  Sign conventions for the 6DX sensor axes are assumed correct
    as supplied. Users must validate SIGN_CORRECTIONS against a
    known impact direction before processing the full dataset.

10. The headform behaves as a rigid body during impact. Structural
    deformation of the headform shell is assumed negligible.

---

## Known Limitations
- Gravity is not removed from the linear acceleration signal.
  See Assumption 6 for full discussion.

- Combined CG shift due to added phone masses is not corrected.
  See Assumption 2 for guidance on quantifying this effect.

- Sign conventions have not been experimentally validated and
  must be confirmed before full dataset processing.

- The rotation matrix R is applied identically to all four phones,
  as they are oriented in the same way. If any phone is mounted at
  a different orientation, a per-phone rotation matrix should be implemented.

---

## Dependencies
- Python    >= 3.8
- numpy     >= 1.21
- pandas    >= 1.3
- scipy     >= 1.7
- openpyxl  >= 3.0  (for .xlsx output)

Install via:
    pip install numpy pandas scipy openpyxl

---

## References
- SAE J211-1: Instrumentation for Impact Test, Part 1 - Electronic
  Instrumentation. SAE International.
- Five-point stencil finite difference: Fornberg, B. (1988).
  Generation of finite difference formulas on arbitrarily spaced
  grids. Mathematics of Computation, 51(184), 699-706.
"""


import numpy as np
import pandas as pd
import glob
import os
from scipy.signal import butter, filtfilt

# =============================================================================
# USER-DEFINED PARAMETERS
# =============================================================================

# --- Input/Output Paths ---
INPUT_FOLDER  = "C:/Users/ceb2317/OneDrive - Imperial College London/Documents/Research/Projects/Schmidt Sciences/Data/PhoneDropTests/RenamedData/Headform_Tests_Renamed/LatestTests/"
OUTPUT_FOLDER = INPUT_FOLDER + "Headform_Transformed_V2/"

# --- Sample Rate ---
FS = 20000.0  # Hz

# --- Filter Parameters ---
# All channels filtered at CFC 1000 (1000 Hz cutoff) per SAE J211
# This applies to: linear acceleration, rotational velocity,
# and derived rotational acceleration
# !! CHANGE FC_LIN AND FC_ROT HERE IF NEEDED !!
FC_LIN       = 1650.0   # Hz - linear acceleration cutoff (SAE J211 CFC 1000)
FC_ROT       = 300.0   # Hz - rotational velocity and acceleration cutoff (SAE J211 CFC 180)
FILTER_ORDER = 4         # Butterworth filter order

# --- Sign Corrections ---
# Set to -1 to flip axis, +1 to keep as-is
# !! VALIDATE AGAINST A KNOWN IMPACT DIRECTION BEFORE PROCESSING FULL DATASET !!
SIGN_CORRECTIONS = {
    "acc_x":  1,
    "acc_y":  1,
    "acc_z":  1,
    "vel_x":  1,
    "vel_y":  1,
    "vel_z":  1,
}

# --- Displacement Vectors: Head CG -> Phone IMU (metres, in HEAD CG frame) ---
# Format: [x, y, z] in the HEAD CG coordinate frame
# Defined as the vector FROM the head CG TO the phone IMU location
# expressed in the head CG sensor coordinate frame (metres)
# !! UPDATE THESE VALUES BEFORE RUNNING !!
PHONE_VECTORS = {
    "001": np.array([-0.057,  -0.016,  -0.1652]),  # Phone 1
    "002": np.array([-0.0514,  0.0309,  0.1552]),  # Phone 2
    "003": np.array([ 0.033,  -0.028,   0.1472]),  # Phone 3
    "004": np.array([-0.0514,  0.0309,  0.1372]),  # Phone 4
}

# --- Coordinate Frame Rotation: Head CG -> Phone ---
# All four phones are assumed to share the same axis
# relative to the headform coordinate frame:
#   phone_x = -head_y
#   phone_y = -head_x
#   phone_z =  head_z
#
# Rotation matrix R such that v_phone = R @ v_head
# !! UPDATE IF PHONE ORIENTATIONS DIFFER FROM HEADFORM !!
R_HEAD_TO_PHONE = np.array([
    [ 0, -1,  0],   # phone_x = -head_y
    [-1,  0,  0],   # phone_y = -head_x
    [ 0,  0,  1],   # phone_z =  head_z
])

# =============================================================================
# HAMPEL FILTER (vectorised)
# =============================================================================

def hampel_filter(x, k=3, nsigma=2):
    """
    Hampel filter for outlier detection and replacement.
    Uses pandas rolling functions for efficiency at high sample rates.

    Parameters
    ----------
    x      : 1D numpy array
    k      : half-window size (full window = 2k+1 samples)
    nsigma : number of scaled MAD standard deviations for threshold

    Returns
    -------
    x_filt : filtered 1D numpy array
    """
    x           = x.copy().astype(float)
    s           = pd.Series(x)
    window      = 2 * k + 1

    rolling_med = s.rolling(window=window, center=True,
                            min_periods=1).median()
    rolling_mad = (s - rolling_med).abs().rolling(
                   window=window, center=True, min_periods=1).median()

    sigma    = 1.4826 * rolling_mad
    outliers = (s - rolling_med).abs() > nsigma * sigma
    x[outliers] = rolling_med[outliers]

    return x

# =============================================================================
# FIVE-POINT STENCIL DERIVATIVE (vectorised)
# =============================================================================

def five_point_stencil(v, fs):
    """
    Computes the derivative of a 1D signal using a five-point stencil
    for interior points, with forward/backward differences at boundaries.
    Fully vectorised for efficiency at high sample rates.

    Parameters
    ----------
    v  : 1D numpy array (signal to differentiate)
    fs : sample rate in Hz (used to compute time step h = 1/fs)

    Returns
    -------
    dv : 1D numpy array (derivative of v)

    Formulae
    --------
    Interior (i = 2..N-3), 4th order accurate:
        dv[i] = (-v[i+2] + 8v[i+1] - 8v[i-1] + v[i-2]) / (12h)

    Boundaries:
        dv[0]   = (v[1]   - v[0])   / h        (1st order forward)
        dv[1]   = (v[2]   - v[0])   / (2h)     (2nd order central)
        dv[N-2] = (v[N-1] - v[N-3]) / (2h)     (2nd order central)
        dv[N-1] = (v[N-1] - v[N-2]) / h        (1st order backward)
    """
    h  = 1.0 / fs
    N  = len(v)
    dv = np.zeros(N)

    # Boundary points
    dv[0]   = (v[1]   - v[0])   / h
    dv[1]   = (v[2]   - v[0])   / (2 * h)
    dv[N-2] = (v[N-1] - v[N-3]) / (2 * h)
    dv[N-1] = (v[N-1] - v[N-2]) / h

    # Interior points - vectorised
    dv[2:N-2] = (-v[4:N] + 8*v[3:N-1] - 8*v[1:N-3] + v[0:N-4]) / (12 * h)

    return dv

# =============================================================================
# READ INPUT FILE
# =============================================================================

def read_impact_file(filepath):
    """
    Reads a headform impact .csv file, skipping metadata rows.
    Locates the 'Data Starts Here' marker row and reads numeric data below it.

    Parameters
    ----------
    filepath : str, path to .csv file

    Returns
    -------
    df : pd.DataFrame with columns:
         [Time, vel_z, vel_y, vel_x, acc_z, acc_y, acc_x]
         in original units (deg/s and g)
    """
    # Read entire file as strings to locate the data start marker
    raw = pd.read_csv(filepath, header=None, dtype=str)

    # Find the row index containing "Data Starts Here"
    data_start_row = None
    for i, row in raw.iterrows():
        if row.astype(str).str.contains("Data Starts Here", case=False).any():
            data_start_row = i
            break

    if data_start_row is None:
        raise ValueError(f"Could not find 'Data Starts Here' in {filepath}")

    # Row layout:
    #   data_start_row     : "Data Starts Here"
    #   data_start_row + 1 : column headers (Time, Chan 0, Chan 1, ...)
    #   data_start_row + 2 : first numeric data row
    header_row     = data_start_row + 1
    first_data_row = data_start_row + 2

    # Extract column names from header row
    col_names = raw.iloc[header_row].tolist()

    # Extract all data rows and reset index
    df = raw.iloc[first_data_row:].reset_index(drop=True).copy()
    df.columns = col_names

    # Drop unnamed or fully empty columns (trailing delimiters)
    df = df.loc[:, ~df.columns.astype(str).str.contains(
        "^Unnamed|^nan", case=False, na=True)]
    df = df.dropna(axis=1, how="all")

    # Convert all columns to numeric
    df = df.apply(pd.to_numeric, errors='coerce')

    # Drop any rows that are entirely NaN (blank trailing rows)
    df = df.dropna(how="all").reset_index(drop=True)

    # Verify column count
    if df.shape[1] != 7:
        raise ValueError(
            f"Expected 7 columns (Time + 6 channels) after cleaning, "
            f"but found {df.shape[1]}. Check CSV formatting in {filepath}."
        )

    # Rename to standard internal names
    # Original order: Time | AV-Z | AV-Y | AV-X | AC-Z | AC-Y | AC-X
    df.columns = ["Time", "vel_z", "vel_y", "vel_x", "acc_z", "acc_y", "acc_x"]

    return df

# =============================================================================
# UNIT CONVERSION AND SIGN CORRECTION
# =============================================================================

def convert_units(df, sign_corrections):
    """
    Converts raw sensor units to SI units and applies axis sign corrections.

    Conversions:
        Rotational velocity : deg/s -> rad/s  (x pi/180)
        Linear acceleration : g    -> m/s²   (x 9.81)

    Sign corrections are applied after unit conversion.
    Validate SIGN_CORRECTIONS against a known impact before full processing.

    Parameters
    ----------
    df               : pd.DataFrame with vel_x/y/z (deg/s), acc_x/y/z (g)
    sign_corrections : dict of {column_name: +1 or -1}

    Returns
    -------
    df : pd.DataFrame in SI units with sign corrections applied
    """
    DEG_TO_RAD = np.pi / 180.0
    G_TO_MS2   = 9.81

    # Unit conversion
    for axis in ["x", "y", "z"]:
        df[f"vel_{axis}"] = df[f"vel_{axis}"] * DEG_TO_RAD
        df[f"acc_{axis}"] = df[f"acc_{axis}"] * G_TO_MS2

    # Sign corrections
    for col, sign in sign_corrections.items():
        if col in df.columns:
            df[col] = df[col] * sign

    return df

# =============================================================================
# FILTER SIGNALS
# =============================================================================

def filter_signals(df, fs, fc_lin, fc_rot, filter_order):
    """
    Applies Hampel outlier removal followed by zero-phase Butterworth
    low-pass filtering to all signals.

    Processing order:
        1. Hampel filter (outlier removal) - all channels
        2. Low-pass Butterworth filter     - linear acceleration (fc_lin)
        3. Low-pass Butterworth filter     - rotational velocity (fc_rot)

    NOTE: Gravity is NOT removed prior to filtering. The linear
    acceleration signal retains its gravitational component throughout
    all subsequent processing and transformation steps.

    Parameters
    ----------
    df           : pd.DataFrame in SI units
    fs           : sample rate (Hz)
    fc_lin       : linear acceleration cutoff frequency (Hz) - SAE J211 CFC1000
    fc_rot       : rotational velocity cutoff frequency (Hz) - SAE J211 CFC1000
    filter_order : Butterworth filter order (4th order)

    Returns
    -------
    df : pd.DataFrame with filtered signals
    """
    # Build filters
    b_lin, a_lin = butter(filter_order, fc_lin / (fs / 2), btype='low')
    b_rot, a_rot = butter(filter_order, fc_rot / (fs / 2), btype='low')

    # Step 1: Hampel filter - outlier removal on all channels
    for col in ["acc_x", "acc_y", "acc_z", "vel_x", "vel_y", "vel_z"]:
        df[col] = hampel_filter(df[col].values, k=3, nsigma=2)

    # Step 2: Low-pass filter linear acceleration (CFC 1000 / 1000 Hz)
    for col in ["acc_x", "acc_y", "acc_z"]:
        df[col] = filtfilt(b_lin, a_lin, df[col].values)

    # Step 3: Low-pass filter rotational velocity (CFC 1000 / 1000 Hz)
    for col in ["vel_x", "vel_y", "vel_z"]:
        df[col] = filtfilt(b_rot, a_rot, df[col].values)

    return df

# =============================================================================
# DERIVE ROTATIONAL ACCELERATION
# =============================================================================

def derive_rotational_acceleration(df, fs, b_rot, a_rot):
    """
    Derives rotational acceleration from filtered rotational velocity
    using the five-point stencil finite difference method, then filters
    the result at the same cutoff as rotational velocity (CFC 1000).

    Processing order:
        1. Five-point stencil differentiation of filtered rotational velocity
        2. Low-pass Butterworth filter on derived rotational acceleration

    Parameters
    ----------
    df     : pd.DataFrame containing filtered vel_x, vel_y, vel_z (rad/s)
    fs     : sample rate (Hz)
    b_rot  : Butterworth filter numerator coefficients
    a_rot  : Butterworth filter denominator coefficients

    Returns
    -------
    df : pd.DataFrame with added columns ang_x, ang_y, ang_z (rad/s²)
    """
    for axis in ["x", "y", "z"]:
        vel_col = f"vel_{axis}"
        ang_col = f"ang_{axis}"

        # Step 1: Derive angular acceleration from filtered velocity
        df[ang_col] = five_point_stencil(df[vel_col].values, fs)

        # Step 2: Filter derived angular acceleration (CFC 300 / 1000 Hz) NOT DOING AS WOULD DOUBLE FILTER, See Baker et al. 2025 (ABME paper)
        # df[ang_col] = filtfilt(b_rot, a_rot, df[ang_col].values)

    return df

# =============================================================================
# RIGID BODY TRANSFORMATION: HEAD CG -> PHONE LOCATION
# =============================================================================

def transform_to_phone(df, r_vec, R):
    """
    Transforms linear acceleration from head CG to a phone IMU location
    using the rigid body equation, then rotates all signals into the
    phone coordinate frame.

    Rigid body equation (in head CG frame):
        a_phone = a_CG + alpha x r + omega x (omega x r)

    where:
        a_CG  = linear acceleration at head CG (includes gravity)
        alpha = angular acceleration vector (rad/s²)
        omega = angular velocity vector (rad/s)
        r     = displacement vector from head CG to phone IMU (metres)

    NOTE: Gravity is present in a_CG and is therefore also present in
    the transformed output a_phone. No gravity correction is applied.

    NOTE: The combined CG of the headform and mounted phones may differ
    from the headform CG alone. This offset is not corrected here.

    All signals are then rotated into the phone coordinate frame:
        v_phone = R @ v_head

    Parameters
    ----------
    df    : pd.DataFrame with filtered/derived head CG signals in SI units
    r_vec : np.array [x, y, z], displacement from head CG to phone (metres),
            expressed in HEAD CG coordinate frame
    R     : np.array (3x3), rotation matrix from head CG to phone frame

    Returns
    -------
    out_df : pd.DataFrame with transformed signals in phone frame
    """
    # Extract head CG signals as (N, 3) arrays
    acc = np.column_stack([df["acc_x"].values,
                           df["acc_y"].values,
                           df["acc_z"].values])   # (N, 3) m/s²

    vel = np.column_stack([df["vel_x"].values,
                           df["vel_y"].values,
                           df["vel_z"].values])   # (N, 3) rad/s

    ang = np.column_stack([df["ang_x"].values,
                           df["ang_y"].values,
                           df["ang_z"].values])   # (N, 3) rad/s²

    # --- Rigid body transformation (vectorised, in head CG frame) ---
    # omega x r : (N, 3)
    cross_wr = np.cross(vel, r_vec)

    # alpha x r : (N, 3)
    cross_ar = np.cross(ang, r_vec)

    # omega x (omega x r) : (N, 3)
    cross_w_wr = np.cross(vel, cross_wr)

    # a_phone = a_CG + alpha x r + omega x (omega x r)
    acc_transformed = acc + cross_ar + cross_w_wr   # (N, 3), still in head frame

    # --- Rotate all signals into phone coordinate frame ---
    # v_phone = R @ v_head, applied row-wise via (R @ M.T).T
    acc_phone = (R @ acc_transformed.T).T   # (N, 3)
    vel_phone = (R @ vel.T).T               # (N, 3)
    ang_phone = (R @ ang.T).T               # (N, 3)

    # --- Compute resultants ---
    acc_res = np.linalg.norm(acc_phone, axis=1)   # (N,)
    vel_res = np.linalg.norm(vel_phone, axis=1)   # (N,)
    ang_res = np.linalg.norm(ang_phone, axis=1)   # (N,)

    # --- Assemble output DataFrame ---
    out_df = pd.DataFrame({
        "Time (s)"           : df["Time"].values,
        "LinAccX (m/s^2)"    : acc_phone[:, 0],
        "LinAccY (m/s^2)"    : acc_phone[:, 1],
        "LinAccZ (m/s^2)"    : acc_phone[:, 2],
        "LinAccRes (m/s^2)"  : acc_res,
        "RotVelX (rad/s)"    : vel_phone[:, 0],
        "RotVelY (rad/s)"    : vel_phone[:, 1],
        "RotVelZ (rad/s)"    : vel_phone[:, 2],
        "RotVelRes (rad/s)"  : vel_res,
        "RotAccX (rad/s^2)"  : ang_phone[:, 0],
        "RotAccY (rad/s^2)"  : ang_phone[:, 1],
        "RotAccZ (rad/s^2)"  : ang_phone[:, 2],
        "RotAccRes (rad/s^2)": ang_res,
    })

    return out_df

# =============================================================================
# MAIN PROCESSING LOOP
# =============================================================================

def main():

    # Create output folder if it doesn't exist
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    # Build rotational filter once (shared for velocity and acceleration)
    b_rot, a_rot = butter(FILTER_ORDER, FC_ROT / (FS / 2), btype='low')

    # Find all .csv files in input folder
    input_files = sorted(glob.glob(os.path.join(INPUT_FOLDER, "*.csv")))

    if not input_files:
        print(f"No .csv files found in '{INPUT_FOLDER}'. Exiting.")
        return

    print(f"Found {len(input_files)} impact file(s) to process.\n")

    for filepath in input_files:

        filename = os.path.splitext(os.path.basename(filepath))[0]
        print(f"Processing: {filename}")

        # --- Read and prepare data ---
        try:
            df = read_impact_file(filepath)
        except Exception as e:
            print(f"  ERROR reading file: {e}")
            continue

        df = convert_units(df, SIGN_CORRECTIONS)
        df = filter_signals(df, FS, FC_LIN, FC_ROT, FILTER_ORDER)
        df = derive_rotational_acceleration(df, FS, b_rot, a_rot)

        # --- Normalise output filename ---
        # Handle files without 'Headform' in name (removes capitalised UNFILTERED suffix)

        filename = filename.replace('_UNFILTERED', '').replace('_Unfiltered', '').replace('_unfiltered', '')

        if 'Headform' not in filename:
            filename = filename + '_Headform'

        # --- Transform to each phone location and write output ---
        for phone_id, r_vec in PHONE_VECTORS.items():

            phone_df = transform_to_phone(df.copy(), r_vec, R_HEAD_TO_PHONE)

            # Output: {original_name}_Transformed_Phone{id}.xlsx
            out_filename = f"{filename}_Transformed_Phone{phone_id}.xlsx"
            out_path     = os.path.join(OUTPUT_FOLDER, out_filename)

            phone_df.to_excel(out_path, index=False)
            print(f"  Written: {out_filename}")

        print(f"  Done.\n")

    print("All files processed successfully.")

# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    main()
