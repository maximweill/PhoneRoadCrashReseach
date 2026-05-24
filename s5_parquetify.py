import pandas as pd
from pathlib import Path
from typing import Optional, Union

def convert_csv_to_parquet(csv_file: Path, pq_file: Path, downsample: int = 1, nrows: Optional[int] = None) -> None:
    """
    Converts a single CSV file to Parquet format.
    """
    try:
        # Some logs might have weird encoding
        try:
            df = pd.read_csv(csv_file, nrows=nrows)
        except UnicodeDecodeError:
            df = pd.read_csv(csv_file, nrows=nrows, encoding='latin1')
            
        # Simple downsampling if requested
        if downsample > 1:
            df = df.iloc[::downsample, :].reset_index(drop=True)
            
        pq_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(pq_file, engine='pyarrow', index=False, compression='snappy')
        print(f"  Converted {csv_file.name} -> {pq_file.name}")
    except Exception as e:
        print(f"  Error converting {csv_file.name}: {e}")

def convert_csv_dir_to_parquet(
    source_dir: Union[str, Path], 
    output_dir: Union[str, Path], 
    downsample: int = 1, 
    nrows: Optional[int] = None,
    rename_map: Optional[dict] = None
) -> None:
    """
    Converts all CSV files in a directory to Parquet format.
    """
    src_path = Path(source_dir)
    dest_path = Path(output_dir)
    
    if not src_path.exists():
        print(f"Source directory {src_path} not found. Skipping.")
        return

    dest_path.mkdir(parents=True, exist_ok=True)
    
    csv_files = list(src_path.glob("*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {src_path}")
        return
    
    print(f"Processing CSVs from {src_path}...")
    for csv_file in csv_files:
        new_name = csv_file.with_suffix(".parquet").name
        if rename_map and csv_file.name in rename_map:
            new_name = rename_map[csv_file.name]
            if not new_name.endswith(".parquet"):
                new_name += ".parquet"
        
        pq_file = dest_path / new_name
        convert_csv_to_parquet(csv_file, pq_file, downsample=downsample, nrows=nrows)

if __name__ == "__main__":
    # Base directories
    DATA_DIR = Path("data_processing_gitignore")
    WEBAPP_DATA_DIR = Path("my_app/data")
    LOGS_DEST = WEBAPP_DATA_DIR / "lookup_tables_parquet"
    

    # # 1. Car Crash Data
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "car_crash_data" / "parsed",
    #     output_dir=WEBAPP_DATA_DIR / "car_crash_data_parquet"
    # )


    # # Logs from lookup_tables
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "lookup_tables",
    #     output_dir=LOGS_DEST,
    #     rename_map={"data_collection_log(Maxim Tests).csv": "data_collection_log.parquet"}
    # )
    
    # Aggregated characteristics
    convert_csv_dir_to_parquet(
        source_dir=DATA_DIR / "phone_characteristics" / "aggregated",
        output_dir=LOGS_DEST,
        rename_map={"characteristics_drops.csv": "phone_characteristics_aggregated.parquet"}
    )

    # # 3. Phone Drop Test Data - Reference Signals
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "phone_drop_test_data" / "reference",
    #     output_dir=WEBAPP_DATA_DIR / "phone_drop_test_data_parquet" / "phone_reference_signals"
    # )
    
    # # 4. Phone Drop Test Data - Framed
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "phone_drop_test_data" / "framed", 
    #     output_dir=WEBAPP_DATA_DIR / "phone_drop_test_data_parquet" / "phone_framed"
    # )

    # 5. Stationary Data start
    convert_csv_dir_to_parquet(
        source_dir=DATA_DIR / "stationary" / "start" / "parsed",
        output_dir=WEBAPP_DATA_DIR / "stationary_parquet" / "start" / "parsed",
        nrows=1_000
    )
    convert_csv_dir_to_parquet(
        source_dir=DATA_DIR / "stationary"/ "start" / "allan_variance",
        output_dir=WEBAPP_DATA_DIR / "stationary_parquet" / "start" / "allan_variance"
    )

    # 6. Stationary Data end
    convert_csv_dir_to_parquet(
        source_dir=DATA_DIR / "stationary" / "end" / "parsed",
        output_dir=WEBAPP_DATA_DIR / "stationary_parquet" / "end" / "parsed",
        nrows=1_000
    )
    convert_csv_dir_to_parquet(
        source_dir=DATA_DIR / "stationary"/ "end" / "allan_variance",
        output_dir=WEBAPP_DATA_DIR / "stationary_parquet" / "end" / "allan_variance"
    )

    # # 7. 6-Axis Calibration
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "6axis_calibaration" / "parsed",
    #     output_dir=WEBAPP_DATA_DIR / "6axis_calibaration_parquet"
    # )
