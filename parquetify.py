import pandas as pd
from pathlib import Path
import time

def convert_csv_to_parquet(source_dir="data", output_dir="data_parquet"):
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
        df = pd.read_csv(csv_file)
        df.to_parquet(pq_file, engine='pyarrow', index=False, compression='snappy')
        
        elapsed = time.time() - start_time
        print(f" {csv_file.name} -> {pq_file.name} ({elapsed:.2f}s)")

if __name__ == "__main__":
    convert_csv_to_parquet(source_dir="data", output_dir="data_parquet")