import pandas as pd
import numpy as np
from pathlib import Path
from scipy import signal

def reformat_csv(csv_file):
    df = pd.read_csv(csv_file, skiprows=22)
    df.columns = df.columns.str.strip()
    df['Time'] = (df['Time'] * 1e9).astype(int)

    #iso naming
    rename_map = {
        'Chan 0:6DX0855-AV1': 'gyroZ_dps',
        'Chan 1:6DX0855-AV2': 'gyroY_dps',
        'Chan 2:6DX0855-AV3': 'gyroX_dps',
        'Chan 3:6DX0855-AC1': 'accelZ_g',
        'Chan 4:6DX0855-AC2': 'accelY_g',
        'Chan 5:6DX0855-AC3': 'accelX_g',
        'Time': 'time_ns'
    }
    df = df.rename(columns=rename_map)
    ##match with phones
    rename_map = {
        'gyroZ_dps': 'gyroZ_dps',
        'gyroX_dps': 'gyroY_dps',
        'gyroY_dps': 'gyroX_dps',
        'accelZ_g': 'accelZ_g',
    }
    df = df.rename(columns={
        'gyroX_dps': 'gyroX_tmp',
        'gyroY_dps': 'gyroY_tmp'
    })
    df = df.rename(columns={
        'gyroX_tmp': 'gyroY_dps',
        'gyroY_tmp': 'gyroX_dps'
    })
    df = df.rename(columns=rename_map)
    
    df['gyroZ_dps'] *= 1 #??
    df['accelZ_g'] *= 1 #??
    df['gyroX_dps'] *= -1
    df['accelX_g'] *= -1
    df['gyroY_dps'] *= -1
    df['accelY_g'] *= -1

    df['accelMag_g'] = np.sqrt(df['accelX_g']**2 + df['accelY_g']**2 + df['accelZ_g']**2)
    df['gyroMag_dps'] = np.sqrt(df['gyroX_dps']**2 + df['gyroY_dps']**2 + df['gyroZ_dps']**2)
    return df


def resample_signal(df, target_freq_hz):
    """
    Resamples a dataframe to a target frequency.
    Assumes time_ns is the time column.
    """
    if df.empty:
        return df
        
    duration_ns = df["time_ns"].iloc[-1] - df["time_ns"].iloc[0]
    num_samples = num_samples = int(np.round(duration_ns * 1e-9 * target_freq_hz)) + 1
    
    if num_samples <= 1:
        return df

    # Exclude time column for resampling
    data_cols = [c for c in df.columns if c != "time_ns"]
    resampled_data = {}
    
    # Resample each column
    for col in data_cols:
        resampled_data[col] = signal.resample(df[col].values, num_samples)
    
    # Reconstruct time axis
    resampled_data["time_ns"] = np.linspace(df["time_ns"].iloc[0], df["time_ns"].iloc[-1], num_samples)
    
    return pd.DataFrame(resampled_data)

def create_reference_signals():
    # Define paths using raw CSV locations
    log_path = Path("test_log_ignore/Data Collection Log.csv")
    headform_dir = Path("phone_drop_test_data_ignore/headform")
    phone_dir = Path("phone_drop_test_data_ignore/phone_cleaned") 
    output_dir = Path("phone_drop_test_data_ignore/phone_reference_signals")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if not log_path.exists():
        print(f"Error: Log file not found at {log_path}")
        return

    # Load the CSV log
    df_log = pd.read_csv(log_path)
    groups = df_log.groupby("Test Name")
    
    total_created = 0
    
    for test_name, group in groups:
        headform_files = group[group["File Name"].str.contains("_FILTERED", na=False)]["File Name"].tolist()
        phone_files = group[group["File Name"].str.contains("crash_data", na=False)]["File Name"].tolist()
        
        if not headform_files or not phone_files:
            continue
            
        # Raw headform data is in CSV
        headform_path = headform_dir / f"{headform_files[0]}.csv"
        if not headform_path.exists():
            continue
            
        # Load raw headform CSV
        # Note: Based on parquetify.py, headform CSVs have 22 header rows
        try:
            df_ref_raw = reformat_csv(headform_path)
            
        except Exception as e:
            print(f"  Error reading headform {headform_path.name}: {e}")
            continue
        
        for phone_stem in phone_files:
            phone_path = phone_dir / f"{phone_stem}.csv"
            if not phone_path.exists():
                phone_path = Path("phone_drop_test_data_ignore/phone") / f"{phone_stem}.csv"
            
            if not phone_path.exists():
                print(f"  Warning: Phone file {phone_stem} not found.")
                continue

            # 1. Calculate average sampling rate of the phone
            df_phone = pd.read_csv(phone_path)
            time_col = "sensor_time_ns" if "sensor_time_ns" in df_phone.columns else "time_ns"
            
            time_diffs = np.diff(df_phone[time_col].values) * 1e-9 
            avg_dt = np.median(time_diffs)
            target_freq = 1.0 / avg_dt
            
            # 2. Resample reference to match phone frequency
            df_ref_resampled = resample_signal(df_ref_raw, target_freq)
            
            # 3. Save as CSV
            dest_path = output_dir / f"{phone_stem}_REF.csv"
            df_ref_resampled.to_csv(dest_path, index=False)
            
            print(f"  Created {dest_path.name} (Resampled to {target_freq:.2f} Hz)")
            total_created += 1
                
    print(f"\nProcess complete. Total reference signals created: {total_created}")

if __name__ == "__main__":
    create_reference_signals()
