from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from shiny import ui, reactive, render
from shinywidgets import output_widget, render_plotly
import re

from .standard_filter import get_drop_index_filters, filter_drop_index_by_input, sorted_strings, DROP_INDEX

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
AGREEMENT_PATH = DATA_DIR / "phone_drop_test_data_parquet" / "agreement" / "agreement.parquet"

CATEGORIES = {
    "LinAcc": ["LinAccX (m/s2)", "LinAccY (m/s2)", "LinAccZ (m/s2)", "LinAccRes (m/s2)"],
    "RotVel": ["RotVelX (rad/s)", "RotVelY (rad/s)", "RotVelZ (rad/s)", "RotVelRes (rad/s)"],
    "RotAcc": ["RotAccX (rad/s2)", "RotAccY (rad/s2)", "RotAccZ (rad/s2)", "RotAccRes (rad/s2)"]
}

def load_processed_agreement() -> pd.DataFrame:
    if not AGREEMENT_PATH.exists():
        return pd.DataFrame()
    df = pd.read_parquet(AGREEMENT_PATH)
    
    def parse_path(p):
        filename = Path(p).stem
        match = re.match(r"(\d+)mps_(.*)_REPEAT(\d+)_(Phone\d+)", filename)
        if match:
            return match.groups()
        return None, None, None, None

    parsed = df["input_path"].apply(parse_path)
    df["target_speed_mps_str"] = parsed.apply(lambda x: str(x[0]) if x[0] is not None else None)
    df["config"] = parsed.apply(lambda x: x[1])
    df["repeat_str"] = parsed.apply(lambda x: str(x[2]) if x[2] is not None else None)
    df["phone_id"] = parsed.apply(lambda x: x[3])
    
    return df

AGREEMENT_DF = load_processed_agreement()

def sensor_agreement_page():
    
    sidebar_cards = get_drop_index_filters("agree")

    return ui.nav_panel(
        "Sensor Agreement",
        ui.layout_sidebar(
            ui.sidebar(
                ui.input_action_button(
                    "update_agree_plots",
                    "Update Plots",
                    class_="btn-primary w-100",
                ),
                ui.hr(),
                *sidebar_cards,
                width=350,
            ),
            # --- Plot Section ---
            ui.layout_columns(
                ui.card(
                    ui.card_header("ICC Forest Plots"),
                    # Explicit pixel height ensures all 3 subplots render completely
                    output_widget("icc_forest_plot", height="1000px"), 
                    full_screen=True,
                    fill=True,
                ),
                ui.card(
                    ui.card_header("Pearson R vs ICC"),
                    # Matches the visual scale of the forest plot next to it
                    output_widget("pearson_icc_plot", height="1000px"),
                    full_screen=True,
                    fill=True,
                ),
                col_widths=[8, 4],
                fill=False, # Prevents this row from getting vertically squished
            ),
            # --- Agreement Metrics Table ---
            ui.card(
                ui.card_header("Agreement Metrics"),
                ui.output_data_frame("agree_table"),
                full_screen=True,
                fill=False, # Keeps the table confined to its data height
            ),
        )
    )

def register_sensor_agreement_server(input, output, session):
    
    @reactive.calc
    @reactive.event(input.update_agree_plots, ignore_none=False)
    def selected_drops():
        return filter_drop_index_by_input(input, "agree", "agree_phone_id")

    @reactive.calc
    def filtered_data():
        drops = selected_drops()
        if drops.empty or AGREEMENT_DF.empty:
            return pd.DataFrame()
        
        # Merge to filter AGREEMENT_DF based on selected drops
        df = AGREEMENT_DF.merge(
            drops[["target_speed_mps_str", "config", "repeat_str", "phone_id"]],
            on=["target_speed_mps_str", "config", "repeat_str", "phone_id"],
            how="inner"
        )
        
        df["label"] = df["phone_id"] + " (" + df["target_speed_mps_str"] + "m/s, " + df["config"] + ", R" + df["repeat_str"] + ")"
        return df

    @render.data_frame
    def agree_table():
        return render.DataTable(filtered_data())

    @output
    @render_plotly
    def icc_forest_plot():
        df = filtered_data()
        if df.empty:
            return px.scatter(title="No data for selected phone")

        # We want to create one subplot per category
        fig = make_subplots(rows=3, cols=1, 
                           shared_xaxes=True,
                           vertical_spacing=0.05,
                           subplot_titles=list(CATEGORIES.keys()))

        for i, (cat_name, sensors) in enumerate(CATEGORIES.items(), 1):
            # For each sensor in category, we have multiple drops
            # We'll plot them all. To make it a "forest plot", 
            # we should probably have sensors on Y axis.
            # But we have multiple drops per sensor.
            
            # Melt the dataframe to get ICC values for these sensors
            icc_cols = [f"{s}_icc" for s in sensors]
            lower_cols = [f"{s}_icc_ci95_lower" for s in sensors]
            upper_cols = [f"{s}_icc_ci95_upper" for s in sensors]
            
            available_sensors = [s for s in sensors if f"{s}_icc" in df.columns]
            
            cat_data = []
            for s in available_sensors:
                s_df = df[["label", f"{s}_icc", f"{s}_icc_ci95_lower", f"{s}_icc_ci95_upper"]].copy()
                s_df.columns = ["label", "icc", "lower", "upper"]
                s_df["sensor"] = s
                cat_data.append(s_df)
            
            if not cat_data:
                continue
                
            cat_df = pd.concat(cat_data)
            
            # To avoid overlapping, we can use 'sensor' as Y and color by 'label'
            # or vice versa. Usually Forest plot has "Study" on Y.
            # Here "Study" is the Drop.
            
            for sensor in available_sensors:
                sensor_df = cat_df[cat_df["sensor"] == sensor]
                
                fig.add_trace(
                    go.Scatter(
                        x=sensor_df["icc"],
                        y=sensor_df["label"] + " | " + sensor,
                        mode="markers",
                        name=sensor,
                        error_x=dict(
                            type='data',
                            symmetric=False,
                            array=sensor_df["upper"] - sensor_df["icc"],
                            arrayminus=sensor_df["icc"] - sensor_df["lower"]
                        ),
                        showlegend=False
                    ),
                    row=i, col=1
                )
            
            fig.add_vline(x=0.75, line_dash="dash", line_color="orange", row=i, col=1)
            fig.add_vline(x=0.90, line_dash="dash", line_color="green", row=i, col=1)

        fig.update_layout(height=1000, title_text="ICC Forest Plots by Category")
        fig.update_xaxes(range=[0, 1], title_text="ICC")
        
        return fig

    @output
    @render_plotly
    def pearson_icc_plot():
        df = filtered_data()
        if df.empty:
            return px.scatter(title="No data for selected phone")
            
        # Melt all sensors to get Pearson R and ICC pairs
        all_pairs = []
        for cat_name, sensors in CATEGORIES.items():
            for s in sensors:
                r_col = f"{s}_pearson_r"
                icc_col = f"{s}_icc"
                if r_col in df.columns and icc_col in df.columns:
                    pair_df = df[["label", r_col, icc_col]].copy()
                    pair_df.columns = ["label", "pearson_r", "icc"]
                    pair_df["sensor"] = s
                    pair_df["category"] = cat_name
                    all_pairs.append(pair_df)
        
        if not all_pairs:
            return px.scatter(title="No Pearson R or ICC columns found")
            
        plot_df = pd.concat(all_pairs)
        
        fig = px.scatter(
            plot_df,
            x="pearson_r",
            y="icc",
            color="category",
            symbol="sensor",
            hover_data=["label"],
            title="Pearson R vs ICC"
        )
        
        fig.add_trace(
            go.Scatter(x=[0, 1], y=[0, 1], mode="lines", name="y=x", line=dict(color="black", dash="dash"))
        )
        
        fig.update_layout(xaxis_range=[0, 1], yaxis_range=[0, 1])
        
        return fig
