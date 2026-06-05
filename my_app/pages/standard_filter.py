from pathlib import Path, PureWindowsPath
import pandas as pd
from shiny import ui, reactive

PAGES_DIR = Path(__file__).resolve().parent
APP_DIR = PAGES_DIR.parent
DATA_DIR = APP_DIR / "data"
DROP_INDEX_PATH = DATA_DIR / "lookup_tables_parquet" / "index" / "drop_file_index.parquet"
CALIB_INDEX_PATH = DATA_DIR / "lookup_tables_parquet" / "index" / "calib_index.parquet"
DROP_PSD_INDEX_PATH = DATA_DIR / "lookup_tables_parquet" / "index" / "drop_psd_index.parquet"


def _path_stem(x):
    return Path(x).stem
def _windows_to_posix(x):
    if pd.notna(x):
        return PureWindowsPath(str(x)).as_posix()
    return x


def load_index(path: Path) -> pd.DataFrame:
    """Loads and prepares an index file for use in the UI."""
    if not path.exists():
        print(f"DEBUG: Index file not found: {path.as_posix()}")
        return pd.DataFrame()
    
    df = pd.read_parquet(path)
    
    # 1. Filter out missing phones safely
    if "phone_id" in df.columns:
        df = df[df["phone_id"].notna()].copy()
    
    # 2. Handle successful entries cleanly
    for col in ["Successful", "successful"]:
        if col in df.columns:
            df[col] = df[col].fillna(False).astype(bool)
            df = df[df[col]]
            break
        
    # 3. Create helper columns for UI selection
    if "target_speed_mps" in df.columns:
        df["target_speed_mps_str"] = df["target_speed_mps"].astype(str)
    if "repeat" in df.columns:
        df["repeat_str"] = df["repeat"].astype(str)
    if "config" not in df.columns and "test_name" in df.columns:
        df["config"] = df["test_name"]
        
    # 4. Create a unique test_id for simple selection lists
    if "Date" in df.columns and "Time" in df.columns:
        df["test_id"] = df["Date"] + " " + df["Time"]
    elif "test_name" in df.columns:
        df["test_id"] = df["test_name"]
    elif "file_name" in df.columns:
        df["test_id"] = df["file_name"].apply(_path_stem)

    # 5. Standardize all paths to posix style
    if "path" in df.columns:
        df["path"] = df["path"].apply(_windows_to_posix)
    print(df.columns)
        
    return df

def sorted_strings(values: pd.Series) -> list[str]:
    return sorted(values.dropna().astype(str).unique().tolist())


def sort_numeric_strings(values: pd.Series) -> list[str]:
    try:
        # Filter out non-numeric looking strings if necessary, or just try-except
        def key_func(value):
            return float(value)

        return sorted(
            values.dropna().astype(str).unique().tolist(),
            key=key_func
        )
    except (ValueError, TypeError):
        return sorted_strings(values)

def get_phone_card(id_prefix: str, df: pd.DataFrame = None, selected_count: int = 1):
    """Generates a checkbox group card for phone selection."""
    if df is None:
        df = DROP_INDEX
        
    if df.empty:
        return ui.card(ui.card_header("Select Phone(s)"), ui.p("No data available"), fill=False)
        
    phone_ids = sorted_strings(df["phone_id"])
    
    # Default to Phone002 if it exists, otherwise use the first n phones
    default_phone = "Phone002"
    if default_phone in phone_ids:
        selected = [default_phone]
    else:
        selected = phone_ids[:selected_count]

    return ui.card(
        ui.card_header("Select Phone(s)"),
        ui.input_checkbox_group(
            f"{id_prefix}_phone_id",
            "",
            choices=phone_ids,
            selected=selected,
        ),
        fill=False,
    )

def get_test_card(id_prefix: str, df: pd.DataFrame = None, label: str = "Select Test(s)"):
    """Generates a single checkbox group card for all tests in the provided index."""
    if df is None:
        df = DROP_INDEX
        
    if df.empty:
        return ui.card(ui.card_header(label), ui.p("No tests available"), fill=False)
        
    test_ids = sorted_strings(df["test_id"])
    return ui.card(
        ui.card_header(label),
        ui.input_checkbox_group(
            f"{id_prefix}_test_id",
            "",
            choices=test_ids,
            selected=test_ids[-1:],
        ),
        fill=False,
    )

def get_filter_cards(id_prefix: str, df: pd.DataFrame = None):
    """Generates checkbox cards for filtering (e.g., by speed and config/repeat)."""
    if df is None:
        df = DROP_INDEX
        
    if df.empty:
        return []
    
    # If we have speed, group by speed (common for drop tests)
    if "target_speed_mps_str" in df.columns:
        unique_combinations = df.drop_duplicates(subset=["target_speed_mps_str", "config", "repeat_str"])
        speeds = sort_numeric_strings(unique_combinations["target_speed_mps_str"])
        cards = []
        
        for speed in speeds:
            speed_df = unique_combinations[unique_combinations["target_speed_mps_str"] == speed]
            drops = speed_df[["config", "repeat_str"]].drop_duplicates().sort_values(["config", "repeat_str"])
            choices = {
                f"{row.config}|{row.repeat_str}": f"{row.config} (R{row.repeat_str})"
                for row in drops.itertuples()
            }
            
            cards.append(
                ui.card(
                    ui.card_header(f"Speed: {speed} m/s"),
                    ui.input_checkbox_group(
                        f"{id_prefix}_choices_{speed}",
                        "",
                        choices=choices,
                        selected=list(choices.keys())[-1:],
                    ),
                    fill=False,
                )
            )
             
        return cards
    
    # Otherwise, group by config (or test_name)
    elif "config" in df.columns:
        configs = sorted_strings(df["config"])
        cards = []
        for config in configs:
            config_df = df[df["config"] == config]
            if "repeat_str" in config_df.columns:
                repeats = sorted_strings(config_df["repeat_str"])
                choices = {f"{config}|{r}": f"Repeat {r}" for r in repeats}
                cards.append(
                    ui.card(
                        ui.card_header(f"Test: {config}"),
                        ui.input_checkbox_group(
                            f"{id_prefix}_choices_{config}",
                            "",
                            choices=choices,
                            selected=list(choices.keys())[-1:],
                        ),
                        fill=False,
                    )
                )
        return cards
        
    return [get_test_card(id_prefix, df)]

def filter_index_by_input(input, id_prefix: str, df: pd.DataFrame = None, phone_input_id: str = None) -> pd.DataFrame:
    """Filters the index DataFrame based on UI selections."""
    if df is None:
        df = DROP_INDEX
        
    if df.empty:
        return pd.DataFrame()

    p_id = phone_input_id or f"{id_prefix}_phone_id"
    selected_phones = input[p_id]()
    if not selected_phones:
        return pd.DataFrame()

    # Priority 1: Check for explicit test_id selection
    if f"{id_prefix}_test_id" in input:
        selected_tests = input[f"{id_prefix}_test_id"]()
        if selected_tests:
            return df[df["phone_id"].isin(selected_phones) & df["test_id"].isin(selected_tests)]

    # Priority 2: Check for grouped selections (speed or config)
    group_col = "target_speed_mps_str" if "target_speed_mps_str" in df.columns else "config"
    groups = df[group_col].unique()
    
    all_selected = []
    for g in groups:
        input_key = f"{id_prefix}_choices_{g}"
        if input_key in input:
            selected = input[input_key]()
            if selected:
                for s in selected:
                    if "|" in s:
                        config, repeat = s.split("|")
                        all_selected.append((g, config, repeat))

    if not all_selected:
        # Final fallback: just phone selection if no test-specific filters found
        return df[df["phone_id"].isin(selected_phones)]

    masks = []
    for group_val, config, repeat in all_selected:
        masks.append(
            (df[group_col] == group_val) & 
            (df["config"] == config) & 
            (df["repeat_str"] == repeat)
        )
    
    filter_mask = masks[0]
    for m in masks[1:]:
        filter_mask |= m
        
    phone_mask = df["phone_id"].isin(selected_phones)
    result = df[filter_mask & phone_mask].copy()
    
    sort_cols = []
    if "target_speed_mps_str" in df.columns: sort_cols.append("target_speed_mps_str")
    if "config" in df.columns: sort_cols.append("config")
    if "repeat_str" in df.columns: sort_cols.append("repeat_str")
    sort_cols.append("phone_id")

    def _sort_key(col):
        if col.name in ["target_speed_mps_str", "repeat_str"]:
            return pd.to_numeric(col, errors="coerce")
        return col

    return result.sort_values(sort_cols, key=_sort_key)

# Compatibility wrappers and unified helpers
def get_unique_drops():
    if DROP_INDEX.empty: return pd.DataFrame()
    return DROP_INDEX.drop_duplicates(subset=["target_speed_mps_str", "config", "repeat_str", "phone_id"])

def get_drop_index_filters(id_prefix: str):
    """Returns a list of UI cards for filtering the drop index."""
    return [get_phone_card(id_prefix, DROP_INDEX), *get_filter_cards(id_prefix, DROP_INDEX)]

def get_calib_index_filters(id_prefix: str):
    """Returns a list of UI cards for filtering the calibration index."""
    return [get_phone_card(id_prefix, CALIB_INDEX), *get_filter_cards(id_prefix, CALIB_INDEX)]

def filter_drop_index_by_input(input, id_prefix: str, phone_input_id: str = None):
    """Filters the drop index based on UI inputs."""
    return filter_index_by_input(input, id_prefix, DROP_INDEX, phone_input_id)

def filter_calib_index_by_input(input, id_prefix: str, phone_input_id: str = None):
    """Filters the calibration index based on UI inputs."""
    return filter_index_by_input(input, id_prefix, CALIB_INDEX, phone_input_id)

# Legacy aliases for compatibility
def get_speed_cards(id_prefix: str):
    return get_filter_cards(id_prefix, DROP_INDEX)


def get_drop_psd_index_filters(id_prefix: str):
    return [get_phone_card(id_prefix, DROP_PSD_INDEX), *get_filter_cards(id_prefix, DROP_PSD_INDEX)]


def filter_drop_psd_index_by_input(input, id_prefix: str, phone_input_id: str = None):
    return filter_index_by_input(input, id_prefix, DROP_PSD_INDEX, phone_input_id)


def get_unique_drop_psd():
    if DROP_PSD_INDEX.empty:
        return pd.DataFrame()
    return DROP_PSD_INDEX.drop_duplicates(
        subset=["target_speed_mps_str", "config", "repeat_str", "phone_id"]
    )

# Pre-loaded indices
DROP_INDEX = load_index(DROP_INDEX_PATH)
CALIB_INDEX = load_index(CALIB_INDEX_PATH)
DROP_PSD_INDEX = load_index(DROP_PSD_INDEX_PATH)