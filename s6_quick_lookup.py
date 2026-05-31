import pandas as pd
from pathlib import Path
from pipelines.naming import parse_convention
from s5_parquetify import LOGS_DEST, WEBAPP_DATA_DIR

def index_with_cleaned_logs(log_df: pd.DataFrame, name: str, srcs: dict[str, Path], output_dir: Path) -> None:
    """Indexes phone data preserving all log columns and adding parquet paths."""
    records = []
    
    # Identify if we are doing drops or calibration based on name
    is_calibration = "calib" in name.lower()
    
    # Pre-process log_df to ensure matching columns have consistent types
    for col in ['target_speed_mps', 'repeat']:
        if col in log_df.columns:
            log_df[col] = pd.to_numeric(log_df[col], errors='coerce')
    
    for data_type, ddir in srcs.items():
        if not ddir.exists():
            print(f"   Warning: Source directory {ddir} not found. Skipping {data_type}.")
            continue
            
        for p in ddir.glob("*.parquet"):
            try:
                meta = parse_convention(p.stem)
            except ValueError:
                # If it doesn't follow convention, try matching by file_name if calibration
                if is_calibration:
                    match = log_df[log_df['file_name'] == p.stem]
                    if not match.empty:
                        row_data = match.iloc[0].to_dict()
                        row_data['data_type'] = data_type
                        row_data['path'] = str(p.relative_to(WEBAPP_DATA_DIR))
                        records.append(row_data)
                continue

            # Matching logic
            if is_calibration and meta.get("repeat") == -1:
                # Robust matching for calibration trigger0 (stationary)
                # Sometimes speed in filename is 0 but log has a timestamp
                match = log_df[
                    (log_df['config'] == meta['config']) &
                    (log_df['repeat'] == meta["repeat"]) &
                    (log_df['phone_id'] == meta['phone_id'])
                ]
            else:
                match = log_df[
                    (log_df['target_speed_mps'] == meta["target_speed_mps"]) &
                    (log_df['config'] == meta['config']) &
                    (log_df['repeat'] == meta["repeat"]) &
                    (log_df['phone_id'] == meta['phone_id'])
                ]
            
            if not match.empty:
                # Take the first match
                row_data = match.iloc[0].to_dict()
                # Merge metadata from filename (useful for 'convention' and 'phone_id' consistency)
                row_data.update(meta)
                row_data['data_type'] = data_type
                # Store path relative to WEBAPP_DATA_DIR for flexibility within the app
                try:
                    row_data['path'] = str(p.relative_to(WEBAPP_DATA_DIR))
                except ValueError:
                    row_data['path'] = str(p)
                    
                records.append(row_data)
    
    if not records:
        print(f"   No records found for {name}")
        return

    df = pd.DataFrame(records)
    
    # Enforce some common types if they exist to match webapp expectations
    if "target_speed_mps" in df.columns:
        df["target_speed_mps"] = pd.to_numeric(df["target_speed_mps"], errors='coerce').fillna(0).astype(int)
    if "repeat" in df.columns:
        df["repeat"] = pd.to_numeric(df["repeat"], errors='coerce').fillna(0).astype(int)
    
    # Map 'successful' to 'Successful' for the webapp's filtering logic
    if "successful" in df.columns:
        df["Successful"] = df["successful"].astype(str).str.upper() == "TRUE"
    elif "Successful" not in df.columns:
        df["Successful"] = True # Default to True if not specified in log
    
    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_dir / f"{name}.parquet", index=False)
    
    # Save CSV for debugging
    debug_dir = Path("data_processing_gitignore/debug_lookup")
    debug_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(debug_dir / f"{name}.csv", index=False)
    
    print(f"   Generated {name}.parquet ({len(df)} entries)")

def generate_quick_lookups(output_dir: Path) -> None:
    """Main entry point to generate all lookup tables."""
    print(f"Generating quick lookups in {output_dir}...")
    
    # Sources are within the webapp data directory (parquet files)
    drop_srcs = {
        "framed": WEBAPP_DATA_DIR / "phone_drop_test_data_parquet" / "phone_framed",
        "reference": WEBAPP_DATA_DIR / "phone_drop_test_data_parquet" / "phone_reference_signals"
    }
    
    drop_log_path = Path("data_processing_gitignore/lookup_tables/data_collection_log.csv")
    if drop_log_path.exists():
        drop_log_df = pd.read_csv(drop_log_path)
        index_with_cleaned_logs(drop_log_df, "drop_file_index", drop_srcs, output_dir)
    else:
        print(f"   Error: Drop log not found at {drop_log_path}")

    calib_srcs = {
        "framed": WEBAPP_DATA_DIR / "6axis_calibration" / "framed"
    }
    
    calib_log_path = Path("data_processing_gitignore/calibration_lookup_tables/data_collection_log.csv")
    if calib_log_path.exists():
        calib_log_df = pd.read_csv(calib_log_path)
        index_with_cleaned_logs(calib_log_df, "calib_index", calib_srcs, output_dir)
    else:
        print(f"   Error: Calibration log not found at {calib_log_path}")

if __name__ == "__main__":
    generate_quick_lookups(
        output_dir=LOGS_DEST / "index",
    )
