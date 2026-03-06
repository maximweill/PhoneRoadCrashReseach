import pandas as pd
import json
import re
from pathlib import Path

# Folder where get_data.py lives
HERE = Path(__file__).resolve().parent


def js_to_dataframe(js_filepath):
    with open(js_filepath, "r", encoding='utf-8') as f:
        js_contents = f.read()

    match = re.search(r'var\s+\w+\s*=\s*(\[.*\]);?', js_contents, flags=re.DOTALL)
    if not match:
        raise ValueError("Cannot find device data array in the file.")

    data_array_str = match.group(1)

    cleaned = (
        data_array_str
        .replace("True", "true")
        .replace("False", "false")
    )
    cleaned = re.sub(r',\s*([}\]])', r'\1', cleaned)
    cleaned = re.sub(r'(\{|,)\s*([a-zA-Z0-9_]+)\s*:', r'\1 "\2":', cleaned)

    data = json.loads(cleaned)
    df = pd.DataFrame(data)
    return df


# FIX: Use explicit absolute paths relative to this file
devices_js_path = HERE / "devices.js"
column_map_path = HERE / "column_map.json"

devices_data = js_to_dataframe(devices_js_path)


def rename_device_columns(d):
    with open(column_map_path) as f:
        column_map = json.load(f)
    return d.rename(columns=column_map)


df_raw = rename_device_columns(devices_data)


def eligible_manufacturers_by_sample_size(data, min_total_sample_size):
    sums = data.groupby("manufacturer")["sample_size"].sum()
    return sums[sums >= min_total_sample_size].index.tolist()


def dedupe_models_keep_max_sample_size(df, model_col="model", sample_col="sample_size"):
    return df.sort_values(sample_col, ascending=False).drop_duplicates(subset=model_col, keep="first")


df_raw = dedupe_models_keep_max_sample_size(df_raw)

MIN_SAMPLE_SIZE = 10
manufacturers = eligible_manufacturers_by_sample_size(df_raw, MIN_SAMPLE_SIZE)
devices_data = df_raw[df_raw["manufacturer"].isin(manufacturers)]

manufacturers = sorted(devices_data['manufacturer'].dropna().unique().tolist(), key=lambda s: s.lower())
numeric_cols = sorted(devices_data.select_dtypes(include='number').columns.tolist())


import pyarrow as pa
import pyarrow.parquet as pq

# 1. Prepare the folder
output_path = HERE / "fast_data"
output_path.mkdir(exist_ok=True)

# 2. Parquet the main dataframe (The Speed Demon Way)
# Using 'brotli' or 'zstd' is smaller, but 'snappy' is faster for raw I/O
devices_data.to_parquet(
    output_path / "devices.parquet", 
    engine="pyarrow", 
    compression="snappy",
    index=False
)

# 3. Parquet the lists (Wrapped in a single-row DataFrame for binary speed)
# This avoids the overhead of the Python json library entirely
pd.DataFrame({
    "manufacturers": [manufacturers],
    "numeric_cols": [numeric_cols]
}).to_parquet(output_path / "metadata.parquet", engine="pyarrow")

print(f"Done. Files saved to {output_path}")