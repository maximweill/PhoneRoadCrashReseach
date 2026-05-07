import pandas as pd
from pathlib import Path

def convert_csv_to_parquet(csv_file, pq_file, downsample=1, skiprows=0):
    try:
        df = pd.read_csv(csv_file, skiprows=skiprows)
        
        # Simple downsampling if requested
        if downsample > 1:
            df = df.iloc[::downsample, :].reset_index(drop=True)
            
        df.to_parquet(pq_file, engine='pyarrow', index=False, compression='snappy')
    except Exception as e:
        print(f"  Error converting {csv_file.name}: {e}")

def convert_csv_dir_to_parquet(source_dir, output_dir, downsample=1, skiprows=0):
    src_path = Path(source_dir)
    dest_path = Path(output_dir)
    
    if not src_path.exists():
        print(f"Source directory {source_dir} not found.")
        return

    dest_path.mkdir(parents=True, exist_ok=True)
    
    csv_files = list(src_path.glob("*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {source_dir}")
        return
    
    for csv_file in csv_files:
        pq_file = dest_path / csv_file.with_suffix(".parquet").name
        convert_csv_to_parquet(csv_file, pq_file, downsample=downsample, skiprows=skiprows)

if __name__ == "__main__":
    # Generic logs and references
    convert_csv_dir_to_parquet(source_dir="test_log_ignore", output_dir="test_log_parquet")
    
    convert_csv_dir_to_parquet(source_dir="phone_drop_test_data_ignore/phone_reference_signals", output_dir="phone_drop_test_data_parquet/phone_reference_signals")
    
    # Final Framed Phone Data
    convert_csv_dir_to_parquet(
        source_dir="phone_drop_test_data_ignore/phone_framed", 
        output_dir="phone_drop_test_data_parquet/phone_framed"
    )
