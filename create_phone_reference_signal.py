import pandas as pd
import numpy as np
from pathlib import Path
import re
from scipy.interpolate import interp1d
from concurrent.futures import ProcessPoolExecutor

def extract_phone_id(filename):
    """
    Extracts PhoneID from filename (e.g., Phone001, Phone_001).
    """
    match = re.search(r"Phone_?(\d+)", filename, re.IGNORECASE)
    if match:
        return f"Phone{match.group(1)}"
    return None

def process_single_file(excel_path, target_fs, output_dir):
    """
    Processes a single headform Excel file: resamples it and saves as CSV.
    """
    try:
        # Load the transformed headform data
        df = pd.read_excel(excel_path)
        
        if "Time (s)" not in df.columns:
            return f"Error: 'Time (s)' column not found in {excel_path.name}"

        time_orig = df["Time (s)"].values
        
        # Define target time axis
        # We preserve the start and end of the original signal
        t_start = time_orig[0]
        t_end = time_orig[-1]
        dt_target = 1.0 / target_fs
        
        time_target = np.arange(t_start, t_end, dt_target)
        
        # Resample all columns except Time
        resampled_data = {"Time (s)": time_target}
        cols_to_resample = [c for c in df.columns if c != "Time (s)"]
        
        for col in cols_to_resample:
            # Linear interpolation
            f = interp1d(time_orig, df[col].values, kind='linear', fill_value="extrapolate")
            resampled_data[col] = f(time_target)
            
        df_resampled = pd.DataFrame(resampled_data)
        
        # Save as CSV
        output_path = output_dir / f"{excel_path.stem}_REF.csv"
        df_resampled.to_csv(output_path, index=False)
        
        return f"Successfully processed {excel_path.name} -> {output_path.name} (FS: {target_fs:.2f} Hz)"
        
    except Exception as e:
        return f"Error processing {excel_path.name}: {e}"

def main():
    headform_dir = Path("phone_drop_test_data_ignore/Transformed_Headform_Data")
    characteristics_path = Path("test_log_ignore/phone_characteristics_aggregated.csv")
    output_dir = Path("phone_drop_test_data_ignore/phone_reference_signals")
    
    if not headform_dir.exists():
        print(f"Error: Directory {headform_dir} does not exist.")
        return

    if not characteristics_path.exists():
        print(f"Error: {characteristics_path} not found. Run phone_characteristics.py first.")
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    # Load target sampling rates
    df_chars = pd.read_csv(characteristics_path)
    if "phone_id" not in df_chars.columns or "fs_median" not in df_chars.columns:
        print("Error: phone_characteristics_aggregated.csv missing required columns (phone_id, fs_median).")
        return
        
    sampling_rates = dict(zip(df_chars["phone_id"], df_chars["fs_median"]))

    # Collect Excel files
    excel_files = list(headform_dir.glob("*.xlsx"))
    if not excel_files:
        print("No Excel files found in Transformed_Headform_Data.")
        return

    print(f"Found {len(excel_files)} files to process.")

    tasks = []
    for excel_path in excel_files:
        phone_id = extract_phone_id(excel_path.name)
        if not phone_id or phone_id not in sampling_rates:
            print(f"Skipping {excel_path.name}: Could not determine Phone ID or no sampling rate found.")
            continue
            
        target_fs = sampling_rates[phone_id]
        tasks.append((excel_path, target_fs, output_dir))

    # Process in parallel
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_single_file, *task) for task in tasks]
        for future in futures:
            print(future.result())

if __name__ == "__main__":
    main()
