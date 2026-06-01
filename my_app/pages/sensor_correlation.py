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
CORRELATION_DIR = DATA_DIR / "phone_drop_test_data_parquet" / "correlation"

SENSOR_OPTIONS = [
    "LinAccX (m/s2)", "LinAccY (m/s2)", "LinAccZ (m/s2)", "LinAccRes (m/s2)",
    "RotVelX (rad/s)", "RotVelY (rad/s)", "RotVelZ (rad/s)", "RotVelRes (rad/s)",
    "RotAccX (rad/s2)", "RotAccY (rad/s2)", "RotAccZ (rad/s2)", "RotAccRes (rad/s2)"
]

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

def sensor_correlation_page():

    sidebar_cards = get_drop_index_filters("corr")

    return ui.nav_panel(
        "Sensor Correlation",
        ui.layout_sidebar(
            ui.sidebar(
                ui.input_action_button(
                    "update_corr_plots",
                    "Update Plots",
                    class_="btn-primary w-100",
                ),
                ui.hr(),
                sidebar_cards[0], # Phone card
                ui.input_select(
                    "corr_sensor",
                    "Select Sensor",
                    choices=SENSOR_OPTIONS,
                    selected=SENSOR_OPTIONS[-1],
                ),
                ui.input_radio_buttons(
                    "corr_plot_type",
                    "Plot Type",
                    choices=["Scatter", "Heatmap"],
                    selected="Scatter",
                    inline=True,
                ),
                *sidebar_cards[1:], # Speed cards
                width=350,
            ),
            # --- CARDS 2 & 3: Grouped Plots side-by-side (or stacked when long) ---
            ui.layout_columns(
                ui.card(
                    ui.card_header("Correlation Plots"),
                    output_widget("correlation_plot", height="100%"),
                    full_screen=True,
                    fill=True,
                ),
                ui.card(
                    ui.card_header("Agreement Trends"),
                    output_widget("agreement_metrics_plot", height="100%"),
                    full_screen=True,
                    fill=True,
                ),
                fill=False,  # <--- Crucial: Prevents this entire row container from squeezing vertically
            ),
            # --- CARD 1: Table ---
            ui.card(
                ui.card_header("Agreement Metrics"),
                ui.output_data_frame("agreement_table"),
                full_screen=True,
                fill=False,  # <--- Crucial: Keeps the table's natural height
            ),
        )
    )

def register_sensor_correlation_server(input, output, session):
    
    @reactive.calc
    @reactive.event(input.update_corr_plots, ignore_none=False)
    def gated_params():
        return {
            "drops": filter_drop_index_by_input(input, "corr", "corr_phone_id"),
            "sensor": input.corr_sensor(),
            "plot_type": input.corr_plot_type(),
        }

    @reactive.calc
    def filtered_agreement():
        params = gated_params()
        drops = params["drops"]
        sensor = params["sensor"]
        
        if drops.empty or AGREEMENT_DF.empty:
            return pd.DataFrame()
        
        # Merge to filter AGREEMENT_DF based on selected drops
        merged = AGREEMENT_DF.merge(
            drops[["target_speed_mps_str", "config", "repeat_str", "phone_id"]],
            on=["target_speed_mps_str", "config", "repeat_str", "phone_id"],
            how="inner"
        )
        
        # Find columns related to the selected sensor
        # They usually start with the sensor name
        relevant_cols = [c for c in merged.columns if c.startswith(sensor) or c in ["target_speed_mps_str", "config", "repeat_str", "phone_id"]]
        
        df = merged[relevant_cols].copy()
        df["label"] = df["phone_id"] + " (" + df["target_speed_mps_str"] + "m/s, " + df["config"] + ", R" + df["repeat_str"] + ")"
        return df

    @render.data_frame
    def agreement_table():
        return render.DataTable(filtered_agreement())

    @output
    @render_plotly
    def agreement_metrics_plot():
        params = gated_params()
        sensor = params["sensor"]
        df = filtered_agreement()
        
        if df.empty:
            return px.scatter(title="No agreement data found")
        
        slope_col = f"{sensor}_slope"
        intercept_col = f"{sensor}_intercept"
        loa_upper_col = f"{sensor}_ba_loa_upper"
        loa_lower_col = f"{sensor}_ba_loa_lower"
        
        # Check if columns exist
        cols = [slope_col, intercept_col, loa_upper_col, loa_lower_col]
        missing = [c for c in cols if c not in df.columns]
        if missing:
            return px.scatter(title=f"Missing columns: {', '.join(missing)}")

        # Create subplots
        fig = make_subplots(rows=2, cols=1, 
                           shared_xaxes=True,
                           vertical_spacing=0.1,
                           subplot_titles=("Slope", "Intercept and Limits of Agreement"))
        
        # Sort by speed then phone then repeat
        df = df.sort_values(["target_speed_mps_str", "phone_id", "repeat_str"])

        # Plot Slope
        fig.add_trace(
            go.Scatter(
                x=df["label"],
                y=df[slope_col],
                mode="markers+lines",
                name="Slope",
                marker=dict(size=10)
            ),
            row=1, col=1
        )
        # Ideal slope line
        fig.add_hline(y=1.0, line_dash="dash", line_color="green", row=1, col=1)

        # Plot Intercept
        fig.add_trace(
            go.Scatter(
                x=df["label"],
                y=df[intercept_col],
                mode="markers+lines",
                name="Intercept (Bias at 0)",
                marker=dict(size=10),
                line=dict(color="blue")
            ),
            row=2, col=1
        )
        
        # Plot LOA Bands as a shaded area
        # We need to be careful with categorical X-axis and fill
        fig.add_trace(
            go.Scatter(
                x=df["label"],
                y=df[loa_upper_col],
                mode="lines",
                name="Upper LOA",
                line=dict(width=0),
                showlegend=False
            ),
            row=2, col=1
        )
        fig.add_trace(
            go.Scatter(
                x=df["label"],
                y=df[loa_lower_col],
                mode="lines",
                name="LOA Range",
                fill='tonexty',
                fillcolor='rgba(128, 128, 128, 0.2)',
                line=dict(width=0)
            ),
            row=2, col=1
        )
        
        # Ideal intercept line
        fig.add_hline(y=0.0, line_dash="dash", line_color="green", row=2, col=1)

        fig.update_layout(height=600, title_text=f"Agreement Metrics: {sensor}")
        fig.update_xaxes(tickangle=45)
        
        return fig

    @reactive.calc
    def correlation_data():
        params = gated_params()
        drops = params["drops"]
        if drops.empty:
            return pd.DataFrame()
        
        all_data = []
        for row in drops.itertuples():
            # Construct filename: {speed}mps_{config}_REPEAT{repeat}_{phone_id}.parquet
            filename = f"{row.target_speed_mps_str}mps_{row.config}_REPEAT{row.repeat_str}_{row.phone_id}.parquet"
            file_path = CORRELATION_DIR / filename
            
            if file_path.exists():
                df = pd.read_parquet(file_path)
                df["target_speed"] = row.target_speed_mps_str
                df["config"] = row.config
                df["repeat"] = row.repeat_str
                df["phone_id"] = row.phone_id
                df["label"] = f"{row.phone_id} ({row.target_speed_mps_str}m/s, {row.config}, R{row.repeat_str})"
                all_data.append(df)
        
        if not all_data:
            return pd.DataFrame()
        
        return pd.concat(all_data, ignore_index=True)

    @output
    @render_plotly
    def correlation_plot():
        params = gated_params()
        sensor = params["sensor"]
        plot_type = params["plot_type"]
        df = correlation_data()
        
        if df.empty:
            return px.scatter(title="No data selected or found")
        
        ref_col = f"{sensor}_ref"
        framed_col = f"{sensor}_framed"
        
        if ref_col not in df.columns or framed_col not in df.columns:
            return px.scatter(title=f"Columns for {sensor} not found in correlation data")
        
        if plot_type == "Scatter":
            # Create a scatter plot Ref vs Framed
            fig = px.scatter(
                df,
                x=ref_col,
                y=framed_col,
                color="label",
                title=f"Correlation: {sensor} (Reference vs Phone)",
                labels={ref_col: "Reference", framed_col: "Phone (Framed)"},
                hover_data=["target_speed", "config", "repeat", "phone_id"]
            )
        else:
            # Create a 2D Heatmap (Density Heatmap)
            fig = px.density_heatmap(
                df,
                x=ref_col,
                y=framed_col,
                title=f"Density Heatmap: {sensor} (Reference vs Phone)",
                labels={ref_col: "Reference", framed_col: "Phone (Framed)"},
                nbinsx=50,
                nbinsy=50,
                marginal_x="histogram",
                marginal_y="histogram"
            )
            # Apply color scale separately to avoid 'V' color bug in marginal histograms
            fig.update_layout(coloraxis_colorscale="Viridis")
            
            # # Ensure each heatmap cell is visually square
            # fig.update_yaxes(
            #     scaleanchor="x",
            #     scaleratio=1,
            # )

        min_val = min(df[ref_col].min(), df[framed_col].min())
        max_val = max(df[ref_col].max(), df[framed_col].max())
        fig.add_trace(
            go.Scatter(
                x=[min_val, max_val],
                y=[min_val, max_val],
                mode="lines",
                name="y=x",
                line=dict(color="black", dash="dash"),
                showlegend=True
            )
        )
        
        return fig
