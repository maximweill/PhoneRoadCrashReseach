import pandas as pd
from pathlib import Path
import re
from pipelines.naming import parse_convention
from s5_parquetify import LOGS_DEST, WEBAPP_DATA_DIR

def index_phone_drop_tests(log_df: pd.DataFrame, src_dir: Path, output_dir: Path) -> None:
    """Indexes phone drop test data preserving native numeric and boolean types."""
    records = []
    drop_dirs = {
        "framed": src_dir / "phone_drop_test_data_parquet" / "phone_framed",
        "reference": src_dir / "phone_drop_test_data_parquet" / "phone_reference_signals"
    }
    
    for data_type, ddir in drop_dirs.items():
        if not ddir.exists():
            continue
        for p in ddir.glob("*.parquet"):
            meta = parse_convention(p.stem)
            meta['data'] = data_type
            meta['path'] = str(p.relative_to(src_dir)).replace("\\", "/")
            
            # 1. Clean up types from filename parser immediately into native types
            # (Assuming parse_convention might extract them as strings or floats)
            target_speed = int(float(meta.get('target_speed_mps', 0)))
            repeat_val = int(float(meta.get('repeat', 0)))
            
            # Update meta dict with real numbers
            meta['target_speed_mps'] = target_speed
            meta['repeat'] = repeat_val
            
            # 2. Match against log_df safely by aligning types during the query
            match = log_df[
                (pd.to_numeric(log_df['target_speed_mps'], errors='coerce') == target_speed) &
                (log_df['config'] == meta['config']) &
                (pd.to_numeric(log_df['repeat'], errors='coerce') == repeat_val) &
                (log_df['phone_id'] == meta['phone_id'])
            ]
            
            if not match.empty:
                meta['time'] = match.iloc[0].get('Test_Time')
                meta['date'] = match.iloc[0].get('Date')
                # Guarantee a strict primitive boolean
                meta['Successful'] = bool(match.iloc[0].get('Successful', False))
            else:
                meta['time'] = meta['date'] = None
                meta['Successful'] = False
                
            records.append(meta)
    
    if records:
        df = pd.DataFrame(records)
        
        # Explicitly enforce correct types on the final output dataframe
        df["target_speed_mps"] = df["target_speed_mps"].astype(int)
        df["repeat"] = df["repeat"].astype(int)
        df["Successful"] = df["Successful"].astype(bool)
        
        output_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_dir / "drop_file_index.parquet", index=False)
        print(f"   Generated phone_drop_test/drop_file_index.parquet ({len(df)} entries)")

def index_car_crash_data(log_df: pd.DataFrame, src_dir: Path, output_dir: Path) -> None:
    """Indexes car crash data."""
    records = []
    crash_dir = src_dir / "car_crash_data_parquet"
    if not crash_dir.exists():
        return
        
    for p in crash_dir.glob("*.parquet"):
        stem = p.stem
        record = {
            "file_name": p.name,
            "path": str(p.relative_to(src_dir)).replace("\\", "/"),
            "phone_id": None, "time": None, "date": None, "config": None
        }
        
        match = log_df[log_df['File_Name'].str.contains(stem, na=False)]
        if not match.empty:
            record["phone_id"] = match.iloc[0].get("phone_id")
            record["time"] = match.iloc[0].get("Test_Time")
            record["date"] = match.iloc[0].get("Date")
            record["config"] = match.iloc[0].get("config")
        
        records.append(record)
            
    if records:
        df = pd.DataFrame(records)
        output_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_dir / "crash_file_index.parquet", index=False)
        print(f"  Generated car_crash/crash_file_index.parquet ({len(df)} entries)")



def index_calibration_data(log_df: pd.DataFrame, src_dir: Path, output_dir: Path) -> None:
    """Indexes 6-axis calibration data using log_df for metadata."""
    records = []
    calib_dir = src_dir / "6axis_calibaration_parquet"
    if not calib_dir.exists():
        return
        
    for p in calib_dir.glob("*.parquet"):
        stem = p.stem
        # Pattern: continuous_20260518_103714_Phone001_trigger1
        match_parts = re.search(r"continuous_(?P<ts>\d{8}_\d{6})_(?P<phone>Phone\d+)_trigger(?P<trig>\d+)", stem)
        
        record = {
            "file_name": p.name,
            "path": str(p.relative_to(src_dir)).replace("\\", "/"),
            "phone_id": None, "time": None, "date": None, "config": None, "trigger": None
        }
        
        if match_parts:
            ts = match_parts.group("ts")
            phone = match_parts.group("phone")
            trig = str(match_parts.group("trig"))
            record["phone_id"] = phone
            record["trigger"] = trig
            
            # If trigger0, match with trigger1 to get base metadata
            search_trig = trig if int(trig) > 0 else 1
            search_name = f"continuous_{ts}_{phone}_trigger{search_trig}"
            
            match = log_df[log_df['File_Name'] == search_name]
            if not match.empty:
                record["date"] = match.iloc[0].get("Date")
                record["config"] = match.iloc[0].get("config")
                
                orig_time = match.iloc[0].get("Test_Time")
                if trig == 0 and orig_time:
                    t = pd.to_datetime(orig_time, format='%H:%M')
                    record["time"] = (t - pd.Timedelta(minutes=10)).strftime('%H:%M')
                else:
                    record["time"] = orig_time
        
        records.append(record)

    if records:
        df = pd.DataFrame(records)
        output_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_dir / "calibration_file_index.parquet", index=False)
        print(f"  Generated calibration/calibration_file_index.parquet ({len(df)} entries)")

def generate_quick_lookups(log_path: Path, output_dir: Path, src_dir: Path) -> None:
    """Main entry point to generate all lookup tables."""
    print(f"Generating quick lookups in {output_dir}...")
    
    log_df = pd.read_parquet(log_path)

    index_phone_drop_tests(log_df, src_dir, output_dir)
    index_car_crash_data(log_df, src_dir, output_dir)
    index_calibration_data(log_df, src_dir, output_dir)

if __name__ == "__main__":
    generate_quick_lookups(
        log_path=LOGS_DEST / "data_collection_log.parquet",
        output_dir=LOGS_DEST / "index",
        src_dir=WEBAPP_DATA_DIR
    )
