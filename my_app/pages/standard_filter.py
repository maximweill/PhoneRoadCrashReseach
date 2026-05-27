from pathlib import Path
import pandas as pd
from shiny import ui, reactive

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
DROP_INDEX_PATH = DATA_DIR / "lookup_tables_parquet" / "index" / "drop_file_index.parquet"

def _load_drop_index() -> pd.DataFrame:
    if not DROP_INDEX_PATH.exists():
        return pd.DataFrame()
    
    df = pd.read_parquet(DROP_INDEX_PATH)
    
    # 1. Filter out missing phones safely
    df = df[df["phone_id"].notna()].copy()
    
    # 2. Handle successful drops cleanly without boolean masking crashes
    if "Successful" in df.columns:
        # Fill missing values with False, convert to explicit boolean type
        df["Successful"] = df["Successful"].fillna(False).astype(bool)
        df = df[df["Successful"]]
        
    # 3. Create helper columns used ONLY by the UI selection components
    # Leaving 'target_speed_mps' and 'repeat' as native integers!
    df["target_speed_mps_str"] = df["target_speed_mps"].astype(str)
    df["repeat_str"] = df["repeat"].astype(str)
        
    return df

DROP_INDEX = _load_drop_index()


def get_unique_drops() -> pd.DataFrame:
    """Returns unique (speed, config, repeat, phone_id) combinations for UI selection."""
    if DROP_INDEX.empty:
        return pd.DataFrame()
    
    subset = ["target_speed_mps_str", "config", "repeat_str", "phone_id"]
    available_subset = [c for c in subset if c in DROP_INDEX.columns]
    return DROP_INDEX.drop_duplicates(subset=available_subset)

def sort_numeric_strings(values: pd.Series) -> list[str]:
    return sorted(values.dropna().astype(str).unique().tolist(), key=lambda value: int(value))

def sorted_strings(values: pd.Series) -> list[str]:
    return sorted(values.dropna().astype(str).unique().tolist())

def get_speed_cards(id_prefix: str, selected_speed: str = "6"):
    """Generates the checkbox cards for each target speed."""
    unique_drops = get_unique_drops()
    if unique_drops.empty:
        return []
    
    speeds = sort_numeric_strings(unique_drops["target_speed_mps_str"])
    speed_cards = []
    
    for speed in speeds:
        speed_df = unique_drops[unique_drops["target_speed_mps_str"] == speed]
        # config + repeat label
        drops = speed_df[["config", "repeat_str"]].drop_duplicates().sort_values(["config", "repeat_str"])
        choices = {
            f"{row.config}|{row.repeat_str}": f"{row.config} (R{row.repeat_str})"
            for row in drops.itertuples()
        }
        
        speed_cards.append(
            ui.card(
                ui.card_header(f"Speed: {speed} m/s"),
                ui.input_checkbox_group(
                    f"{id_prefix}_choices_{speed}",
                    "",
                    choices=choices,
                    selected=list(choices.keys())[:1] if speed == selected_speed else [],
                ),
                fill=False,
            )
        )
    return speed_cards

def filter_log_by_input(input, id_prefix: str, phone_input_id: str) -> pd.DataFrame:
    """Common logic to filter the unique drops based on UI selections."""
    unique_drops = get_unique_drops()
    if unique_drops.empty:
        return pd.DataFrame()

    selected_phones = input[phone_input_id]()
    if not selected_phones:
        return pd.DataFrame()

    speeds = sort_numeric_strings(unique_drops["target_speed_mps_str"])
    all_selected_drops = []
    for speed in speeds:
        selected = input[f"{id_prefix}_choices_{speed}"]()
        if selected:
            for s in selected:
                config, repeat = s.split("|")
                all_selected_drops.append((speed, config, repeat))

    if not all_selected_drops:
        return pd.DataFrame()

    masks = []
    for speed, config, repeat in all_selected_drops:
        masks.append(
            (unique_drops["target_speed_mps_str"] == speed) & 
            (unique_drops["config"] == config) & 
            (unique_drops["repeat_str"] == repeat)
        )
    
    drop_mask = masks[0]
    for m in masks[1:]:
        drop_mask |= m
        
    phone_mask = unique_drops["phone_id"].isin(selected_phones)
    result = unique_drops[drop_mask & phone_mask].copy()
    
    def _sort_key(col):
        if col.name in ["target_speed_mps_str", "repeat_str"]:
            return pd.to_numeric(col, errors="coerce")
        return col

    return result.sort_values(
        ["target_speed_mps_str", "repeat_str", "phone_id"],
        key=_sort_key,
    )
