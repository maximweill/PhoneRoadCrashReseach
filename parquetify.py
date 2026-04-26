import pandas as pd
from pathlib import Path
import time
import numpy as np

empty_df = pd.DataFrame({
                'gyroZ_dps': [0],
                'gyroX_dps': [0],
                'gyroY_dps': [0],
                'accelZ_g': [0],
                'accelX_g': [0],
                'accelY_g': [0],
                "time_ns": [0],
                "accelMag_g" : [0],
                "gyroMag_dps": [0]
            })

def frame_data(df, t0=5e7, tf=50e7, trigger=7.9):
    magnitude2 = df["accelX_g"]**2 + df["accelY_g"]**2 + df["accelZ_g"]**2
    trigger2 = trigger**2

    # .idxmax() on the boolean mask returns the index of the first True
    trigger_idx = (magnitude2 > trigger2).idxmax()
    
    # Check if the first value actually met the condition (idxmax returns 0 if never found)
    if magnitude2.iloc[0] > trigger2:
        if trigger <= 1:
            print("no trigger found--------------------------------------------------------")
            return empty_df
        else:
            print(f"trigger reduced from {trigger} to {trigger-1}-------------------------------------------------------")
            return frame_data(df,trigger=trigger-1)
    else:   
        t_trigger = df.at[trigger_idx, "time_ns"]
        
        # 3. Define the window: [t_trigger - t0, t_trigger + tf]
        start_bound = t_trigger - t0
        end_bound = t_trigger + tf

        df = df[(df["time_ns"] >= start_bound) & (df["time_ns"] <= end_bound)].copy()
        df["time_ns"] -= t_trigger
        
        # 4. Slice and return
        return df

def convert_csv_to_parquet(csv_file="data.csv", pq_file="data_parquet",downsample = 1, framer_df = None):
    if framer_df is not None:
        # Find the row for this file
        params = framer_df[framer_df["phone_file"] == csv_file.name]
        
        if params.empty:
            print(f"  Skipping {csv_file.name}: No framing parameters found")
            return
            
        lag_idx = max(0, int(params.iloc[0]["lag_indices"]))
        ref_length = int(params.iloc[0]["ref_length"])
        lag_time = int(params.iloc[0]["lag_ns"])
        
        df = pd.read_csv(csv_file)
        
        df["time_ns"] += lag_time
        # Slice the data
        df = df.iloc[lag_idx : lag_idx + ref_length].copy()

        df_downsampled = df.iloc[::downsample, :]
        df_downsampled.to_parquet(pq_file, engine='pyarrow', index=False, compression='snappy')
    else:
        df = pd.read_csv(csv_file)
        if "sensor_time_ns" in df.columns:
            df = df.rename(columns={
                "sensor_time_ns":"time_ns",
            })
            df = frame_data(df)
            df_downsampled = df.iloc[::downsample, :]
            df_downsampled.to_parquet(pq_file, engine='pyarrow', index=False, compression='snappy')
        else:
            df.to_parquet(pq_file, engine='pyarrow', index=False, compression='snappy')


def convert_csv_dir_to_parquet(source_dir="data", output_dir="data_parquet",downsample = 1, framer_path = None):
    # Create Path objects
    src_path = Path(source_dir)
    dest_path = Path(output_dir)
    
    # Create the output directory if it doesn't exist
    dest_path.mkdir(parents=True, exist_ok=True)
    
    csv_files = list(src_path.glob("*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {source_dir}")
        return

    print(f" Converting {len(csv_files)} files to {output_dir}...")
    
    framer_df = None
    if framer_path:
        framer_path = Path(framer_path)
        if framer_path.exists():
            framer_df = pd.read_csv(framer_path)
        else:
            print(f"Warning: framer_path '{framer_path}' not found.")

    for csv_file in csv_files:
        pq_file = dest_path / csv_file.with_suffix(".parquet").name
        convert_csv_to_parquet(csv_file=csv_file, pq_file=pq_file,downsample = downsample, framer_df = framer_df)



if __name__ == "__main__":
    convert_csv_dir_to_parquet(source_dir="car_crash_data_ignore", output_dir="car_crash_data_parquet")
    convert_csv_dir_to_parquet(source_dir="test_log_ignore", output_dir="test_log_parquet")
    convert_csv_dir_to_parquet(source_dir="phone_drop_test_data_ignore/phone_reference_signals", output_dir="phone_drop_test_data_parquet/phone_reference_signals")
    convert_csv_dir_to_parquet(source_dir="phone_drop_test_data_ignore/phone_cleaned", output_dir="phone_drop_test_data_parquet/phone_cleaned", framer_path="test_log_ignore/phone_cropping_params.csv")
    