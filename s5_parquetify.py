import pandas as pd
from pathlib import Path
from typing import Optional, Union

def convert_csv_to_parquet(csv_file: Path, pq_file: Path, downsample: int = 1, nrows: Optional[int] = None, max_duplicates:Optional[dict[str,int]] = None) -> None:
    """
    Converts a single CSV file to Parquet format.
    """
    try:
        df = pd.read_csv(
            csv_file,
            comment="#",
            engine="c",
            nrows=nrows
        )
        if "Time (s)" in df.columns:
            df = df.sort_values("Time (s)").reset_index(drop=True)


        if not(max_duplicates is None):
            for col, number in max_duplicates.items():
                df = df.groupby(col, as_index=False, sort=False).head(number)

        # Simple downsampling if requested
        if downsample > 1:
            df = df.iloc[::downsample].reset_index(drop=True)
            
        pq_file.parent.mkdir(parents=True, exist_ok=True)

        #reduce size by float64->32
        float_cols = df.select_dtypes(include=["float"]).columns
        df[float_cols] = df[float_cols].astype("float32")

        df.to_parquet(pq_file, engine='pyarrow', index=False, compression='snappy')
        print(f"  Converted {csv_file.name} -> {pq_file.name}")
    except Exception as e:
        print(f"  Error converting {csv_file.name}: {e}")

def convert_csv_dir_to_parquet(
    source_dir: Union[str, Path], 
    output_dir: Union[str, Path], 
    downsample: int = 1, 
    nrows: Optional[int] = None,
    rename_map: Optional[dict] = None,
    max_duplicates:Optional[dict[str,int]] = None
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
        convert_csv_to_parquet(
            csv_file,
            pq_file,
            downsample=downsample,
            nrows=nrows,
            max_duplicates=max_duplicates
        )



DATA_DIR = Path("data_processing_gitignore")
WEBAPP_DATA_DIR = Path("my_app/data")
LOGS_DEST = WEBAPP_DATA_DIR / "lookup_tables_parquet"
if __name__ == "__main__":
    # # Base directories
    # 1. Car Crash Data
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "car_crash_data" / "parsed",
    #     output_dir=WEBAPP_DATA_DIR / "car_crash_data_parquet",
    #     nrows=20_000,
    #     downsample=100
    # )

    
    # Aggregated characteristics
    convert_csv_dir_to_parquet(
        source_dir=DATA_DIR / "phone_characteristics" / "aggregated",
        output_dir=LOGS_DEST/"characteristics",
        rename_map={"characteristics_drops.csv": "phone_characteristics_aggregated.parquet"}
    )
    # Headform characteristics
    convert_csv_dir_to_parquet(
        source_dir=DATA_DIR / "phone_characteristics" / "headform",
        output_dir=LOGS_DEST/"characteristics",
        rename_map={"characteristics_drops.csv": "headform_characteristics.parquet"}
    )

    # # 3. Phone Drop Test Data - Reference Signals
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "phone_drop_test_data" / "reference",
    #     output_dir=WEBAPP_DATA_DIR / "phone_drop_test_data_parquet" / "phone_reference_signals",
    # )
    # # # 4. Phone Drop Test Data - Framed
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "phone_drop_test_data" / "framed", 
    #     output_dir=WEBAPP_DATA_DIR / "phone_drop_test_data_parquet" / "phone_framed",
    # )
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "phone_drop_test_data" / "correlation", 
    #     output_dir=WEBAPP_DATA_DIR / "phone_drop_test_data_parquet" / "correlation",
    #     downsample=10
    # )
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "phone_drop_test_data" / "agreement", 
    #     output_dir=WEBAPP_DATA_DIR / "phone_drop_test_data_parquet" / "agreement",
    #     downsample=10
    # )


    # # # 5. Stationary Data start
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "stationary" / "start" / "parsed",
    #     output_dir=WEBAPP_DATA_DIR / "stationary_parquet" / "start" / "parsed",
    #     nrows=1_000
    # )
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "stationary"/ "start" / "allan_variance",
    #     output_dir=WEBAPP_DATA_DIR / "stationary_parquet" / "start" / "allan_variance",
    #     downsample=2
    # )
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "stationary"/ "start" / "power_spectral_density",
    #     output_dir=WEBAPP_DATA_DIR / "stationary_parquet" / "start" / "power_spectral_density",
    #     downsample=10
    # )

    # # 6. Stationary Data end
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "stationary" / "end" / "parsed",
    #     output_dir=WEBAPP_DATA_DIR / "stationary_parquet" / "end" / "parsed",
    #     nrows=1_000
    # )
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "stationary"/ "end" / "allan_variance",
    #     output_dir=WEBAPP_DATA_DIR / "stationary_parquet" / "end" / "allan_variance",
    #     downsample=2
    # )
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "stationary"/ "end"/ "power_spectral_density",
    #     output_dir=WEBAPP_DATA_DIR / "stationary_parquet" / "end" / "power_spectral_density",
    #     downsample=10
    # )

    # # # 5. Stationary Data end2
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "stationary" / "end2" / "parsed",
    #     output_dir=WEBAPP_DATA_DIR / "stationary_parquet" / "end2" / "parsed",
    #     nrows=1_000
    # )
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "stationary"/ "end2" / "allan_variance",
    #     output_dir=WEBAPP_DATA_DIR / "stationary_parquet" / "end2" / "allan_variance",
    #     downsample=2
    # )
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "stationary"/ "end2" / "power_spectral_density",
    #     output_dir=WEBAPP_DATA_DIR / "stationary_parquet" / "end2" / "power_spectral_density",
    #     downsample=10
    # )

    # # 7. 6-Axis Calibration
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "6axis_calibration" / "framed",
    #     output_dir=WEBAPP_DATA_DIR / "6axis_calibration" / "framed",
    #     max_duplicates={"axis":20}
    # )
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "6axis_calibration" / "calibration",
    #     output_dir=WEBAPP_DATA_DIR / "6axis_calibration" / "calibration",
    # )

    # # 8. Headform Stationary Data
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "stationary" / "headform" / "parsed",
    #     output_dir=WEBAPP_DATA_DIR / "stationary_parquet" / "headform" / "parsed",
    #     nrows=1_000
    # )
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "stationary"/ "headform" / "allan_variance",
    #     output_dir=WEBAPP_DATA_DIR / "stationary_parquet" / "headform" / "allan_variance",
    #     downsample=2
    # )
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "stationary"/ "headform" / "power_spectral_density",
    #     output_dir=WEBAPP_DATA_DIR / "stationary_parquet" / "headform" / "power_spectral_density",
    #     downsample=10
    # )

    # # 9. Headform Filtered Stationary Data
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "stationary" / "headform_filtered" / "parsed",
    #     output_dir=WEBAPP_DATA_DIR / "stationary_parquet" / "headform_filtered" / "parsed",
    #     nrows=1_000
    # )
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "stationary"/ "headform_filtered" / "allan_variance",
    #     output_dir=WEBAPP_DATA_DIR / "stationary_parquet" / "headform_filtered" / "allan_variance",
    #     downsample=2
    # )
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "stationary"/ "headform_filtered" / "power_spectral_density",
    #     output_dir=WEBAPP_DATA_DIR / "stationary_parquet" / "headform_filtered" / "power_spectral_density",
    #     downsample=10
    # )


    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "phone_drop_test_data"/ "power_spectral_density"/"framed",
    #     output_dir=WEBAPP_DATA_DIR / "drop_psd" / "framed",
    #     downsample=30
    # )
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "phone_drop_test_data"/ "power_spectral_density"/ "headform",
    #     output_dir=WEBAPP_DATA_DIR / "drop_psd" / "headform",
    #     downsample=30
    # )
    # convert_csv_dir_to_parquet(
    #     source_dir=DATA_DIR / "phone_drop_test_data"/ "power_spectral_density"/ "reference",
    #     output_dir=WEBAPP_DATA_DIR / "drop_psd" / "reference",
    #     downsample=30
    # )
