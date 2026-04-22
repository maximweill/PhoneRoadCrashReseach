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


def convert_csv_to_parquet(source_dir="data", output_dir="data_parquet",downsample = 1):
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
    
    for csv_file in csv_files:
        start_time = time.time()
        
        # Define the output path: output_dir/filename.parquet
        pq_file = dest_path / csv_file.with_suffix(".parquet").name
        
        # Read and Write
        # Using engine='pyarrow' for maximum speed
        if "crash_data" in csv_file.name:
            df = pd.read_csv(csv_file)
            df = df.rename(columns={
                "sensor_time_ns":"time_ns",
            })
            df = frame_data(df)
            df_downsampled = df.iloc[::downsample, :]
            df_downsampled.to_parquet(pq_file, engine='pyarrow', index=False, compression='snappy')
        elif "FILTERED" in csv_file.name:
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
                'accelX_g': 'accelY_g',
                'accelY_g': 'accelX_g',
            }
            df = df.rename(columns=rename_map)
            
            df['gyroZ_dps'] *= 1 #??
            df['accelZ_g'] *= 1 #??
            df['gyroX_dps'] *= -1
            df['accelX_g'] *= -1
            df['gyroY_dps'] *= -1
            df['accelY_g'] *= -1

            df['accelMag_g'] = np.sqrt(df['accelX_g']**2 + df['accelY_g']**2 + df['accelZ_g']**2)
            df['gyroMag_dps'] = np.sqrt(df['gyroX_dps']**2 + df['gyroY_dps']**2 + df['gyroZ_dps']**2)

            df = frame_data(df)
            df_downsampled = df.iloc[::downsample, :]
            df_downsampled.to_parquet(pq_file, engine='pyarrow', index=False, compression='snappy')
            

        else:
            df = pd.read_csv(csv_file)
            df.to_parquet(pq_file, engine='pyarrow', index=False, compression='snappy')

        
        elapsed = time.time() - start_time
        print(f" {csv_file.name} -> {pq_file.name} ({elapsed:.2f}s)")

if __name__ == "__main__":
    #convert_csv_to_parquet(source_dir="car_crash_data_ignore", output_dir="car_crash_data_parquet")
    #convert_csv_to_parquet(source_dir="phone_drop_test_data_ignore/headform", output_dir="phone_drop_test_data_parquet/headform_parquet", downsample=5)
    convert_csv_to_parquet(source_dir="phone_drop_test_data_ignore/phone", output_dir="phone_drop_test_data_parquet/phone_parquet")
    #convert_csv_to_parquet(source_dir="test_log_ignore", output_dir="test_log_parquet")