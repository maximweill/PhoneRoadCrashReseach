from pathlib import Path
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from shiny import reactive, render, ui
from shinywidgets import output_widget, render_plotly

from .standard_filter import (
    DATA_DIR,
    CALIB_INDEX,
    filter_calib_index_by_input,
    get_calib_index_filters,
    sorted_strings,
)

CALIBRATION_DIR = DATA_DIR / "6axis_calibration"
PARAMETERS_PATH = CALIBRATION_DIR / "calibration" / "parameters.parquet"

G = 9.80665

PHONE_SYMBOLS = {
    "Phone001": "circle",
    "Phone002": "cross",
    "Phone003": "square",
    "Phone004": "diamond",
    "Phone005": "triangle-up",
    "Phone006": "star",
}

def _get_numeric_time(df):
    if df.empty or "global_time" not in df.columns:
        return pd.Series(0, index=df.index)
    
    # Try common format first: YYYYMMDD_HHMMSS
    s = df["global_time"].astype(str)
    times = pd.to_datetime(s, format="%Y%m%d_%H%M%S", errors='coerce')
    if times.isna().all():
        times = pd.to_datetime(s, errors='coerce')
    
    if times.notna().any():
        # Convert to unix timestamp in seconds
        return pd.to_numeric(times) // 10**9
        
    return pd.to_numeric(df["global_time"], errors='coerce').fillna(0)

def load_parameters() -> pd.DataFrame:
    df = pd.read_parquet(PARAMETERS_PATH)
    return df

PARAMETERS_DF = load_parameters()

def calibration_tests_page():
    if CALIB_INDEX.empty:
        return ui.nav_panel("6-Axis Calibration", ui.p("No calibration data available"))

    sidebar_cards = get_calib_index_filters("cal")

    return ui.nav_panel(
        "6-Axis Calibration",
        ui.layout_sidebar(
            ui.sidebar(
                ui.input_action_button(
                    "update_cal_plots",
                    "Update Plots",
                    class_="btn-primary w-100",
                ),
                ui.hr(),
                *sidebar_cards,
                width=350,
            ),
            ui.layout_columns(
                ui.card(
                    ui.card_header("Accelerometer Scatter (Data)"),
                    output_widget("cal_accel_scatter_plot"),
                    full_screen=True,
                ),
                ui.card(
                    ui.card_header("Accelerometer Parameters (Bias & Scale)"),
                    output_widget("cal_accel_params_plot"),
                    full_screen=True,
                ),
                col_widths=[12, 12],
                fill=False,
            ),
            ui.layout_columns(
                ui.card(
                    ui.card_header("Rotational Velocity 3D Scatter"),
                    output_widget("cal_gyro_3d_plot"),
                    full_screen=True,
                ),
                ui.card(
                    ui.card_header("Rotational Velocity Bias"),
                    output_widget("cal_gyro_bias_plot"),
                    full_screen=True,
                ),
                col_widths=[6, 6],
                fill=False,
            ),
            ui.accordion(
                ui.accordion_panel(
                    "Accelerometer Components",
                    ui.layout_columns(
                        ui.card(
                            ui.card_header("LinAcc X"),
                            output_widget("cal_accel_x_plot"),
                            full_screen=True,
                        ),
                        ui.card(
                            ui.card_header("LinAcc Y"),
                            output_widget("cal_accel_y_plot"),
                            full_screen=True,
                        ),
                        ui.card(
                            ui.card_header("LinAcc Z"),
                            output_widget("cal_accel_z_plot"),
                            full_screen=True,
                        ),
                        col_widths=[4, 4, 4],
                    ),
                ),
                ui.accordion_panel(
                    "Gyroscope Components",
                    ui.layout_columns(
                        ui.card(
                            ui.card_header("RotVel X"),
                            output_widget("cal_gyro_x_plot"),
                            full_screen=True,
                        ),
                        ui.card(
                            ui.card_header("RotVel Y"),
                            output_widget("cal_gyro_y_plot"),
                            full_screen=True,
                        ),
                        ui.card(
                            ui.card_header("RotVel Z"),
                            output_widget("cal_gyro_z_plot"),
                            full_screen=True,
                        ),
                        col_widths=[4, 4, 4],
                    ),
                ),
                open=True,
            ),
        ),
    )

def _plot_dimension(df: pd.DataFrame, col: str, label: str):
    if df.empty:
        return go.Figure().update_layout(title="No data")

    phones = sorted(df["phone_id"].unique().tolist())
    fig = make_subplots(rows=len(phones), cols=1, 
                       subplot_titles=[f"{p}" for p in phones],
                       shared_xaxes=True,
                       vertical_spacing=0.05)

    for r, phone_id in enumerate(phones):
        pdf = df[df["phone_id"] == phone_id]
        if "axis" in pdf.columns:
            for axis_val in sorted(pdf["axis"].unique().tolist()):
                adf = pdf[pdf["axis"] == axis_val]
                fig.add_trace(
                    go.Scatter(x=adf["Time (s)"], y=adf[col], mode='markers', 
                               name=f"{axis_val}", legendgroup=axis_val, showlegend=(r==0)),
                    row=r+1, col=1
                )
        else:
             fig.add_trace(
                go.Scatter(x=pdf["Time (s)"], y=pdf[col], mode='markers', 
                           name=phone_id, legendgroup=phone_id, showlegend=(r==0)),
                row=r+1, col=1
            )
        fig.update_yaxes(title_text=label, row=r+1, col=1)

    fig.update_layout(height=200*max(1, len(phones)), margin=dict(t=40, b=40, l=40, r=40))
    return fig

def register_calibration_tests_server(input, output, session):
    @reactive.calc
    @reactive.event(input.update_cal_plots, ignore_none=False)
    def selected_index():
        return filter_calib_index_by_input(input, "cal")

    @reactive.calc
    def selected_parameters():
        idx = selected_index()
        if idx.empty or PARAMETERS_DF.empty:
            print("EMPTY ALREADY")
            return pd.DataFrame()
        

        merged = pd.merge(
            idx,
            PARAMETERS_DF,
            on=["phone_id", "repeat","target_speed_mps"],
            how="inner",
        )
        # ensure a single clean column
        if "global_time" not in merged.columns:
            if "global_time_x" in merged.columns:
                merged["global_time"] = merged["global_time_x"]

        return merged

    @reactive.calc
    def calibration_data():
        sel_idx = selected_index()
        if sel_idx.empty:
            return pd.DataFrame()
        
        frames = []
        for row in sel_idx.itertuples():
            path = DATA_DIR / row.path
            if path.exists():
                df = pd.read_parquet(path)
                df["phone_id"] = row.phone_id
                # Use global_time as global_time
                df["global_time"] = row.global_time
                frames.append(df)
            else:
                print(f"DEBUG: Calibration data file not found: {path.as_posix()}")
        
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    @output
    @render_plotly
    def cal_accel_scatter_plot():
        df = calibration_data()
        if df.empty:
            return go.Figure().update_layout(title="No data")

        df = df.copy()
        df["global_time_num"] = _get_numeric_time(df)

        fig = make_subplots(rows=1, cols=3, subplot_titles=("AX vs AY", "AY vs AZ", "AZ vs AX"))
        
        pairings = [
            ("LinAccX (m/s2)", "LinAccY (m/s2)"),
            ("LinAccY (m/s2)", "LinAccZ (m/s2)"),
            ("LinAccZ (m/s2)", "LinAccX (m/s2)")
        ]

        theta = np.linspace(0, 2*np.pi, 100)
        circle_x = G * np.cos(theta)
        circle_y = G * np.sin(theta)

        for i, (col_x, col_y) in enumerate(pairings):
            for phone_id in sorted(df["phone_id"].unique()):
                pdf = df[df["phone_id"] == phone_id]
                symbol = PHONE_SYMBOLS.get(phone_id, "circle")
                fig.add_trace(
                    go.Scatter(
                        x=pdf[col_x], y=pdf[col_y], mode='markers', name=phone_id, 
                        legendgroup=phone_id, showlegend=(i==0),
                        marker=dict(
                            color=pdf["global_time_num"],
                            coloraxis='coloraxis',
                            symbol=symbol,
                            size=4,
                            opacity=0.6
                        )
                    ),
                    row=1, col=i+1
                )
            
            fig.add_trace(
                go.Scatter(x=circle_x, y=circle_y, mode='lines', name='g-circle', 
                           line=dict(color='black', dash='dash'), showlegend=(i==0)),
                row=1, col=i+1
            )
            fig.update_xaxes(title_text=col_x, row=1, col=i+1)
            fig.update_yaxes(title_text=col_y, row=1, col=i+1)

        fig.update_layout(
            height=500, title_text="Accelerometer Pairings",
            coloraxis=dict(colorscale='Viridis', colorbar=dict(title="Time")),
            legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5)
        )
        return fig

    @output
    @render_plotly
    def cal_accel_params_plot():
        params = selected_parameters()
        if params.empty:
            return go.Figure().update_layout(title="No data")

        params = params.copy()
        params["global_time_num"] = _get_numeric_time(params)

        fig = make_subplots(rows=1, cols=3, subplot_titles=("AX vs AY Params", "AY vs AZ Params", "AZ vs AX Params"))
        
        pairings = [
            ("LinAccX_offset", "LinAccX_scale", "LinAccY_offset", "LinAccY_scale"),
            ("LinAccY_offset", "LinAccY_scale", "LinAccZ_offset", "LinAccZ_scale"),
            ("LinAccZ_offset", "LinAccZ_scale", "LinAccX_offset", "LinAccX_scale")
        ]

        theta = np.linspace(0, 2*np.pi, 100)

        for i, (ox_col, sx_col, oy_col, sy_col) in enumerate(pairings):
            for row in params.itertuples():
                ox, sx = getattr(row, ox_col), getattr(row, sx_col)
                oy, sy = getattr(row, oy_col), getattr(row, sy_col)
                
                # Ellipse tracing scaling factors
                ex = ox + sx * np.cos(theta)
                ey = oy + sy * np.sin(theta)
                
                fig.add_trace(
                    go.Scatter(
                        x=ex, y=ey, mode='lines',
                        line=dict(color='rgba(150, 150, 150, 0.5)', width=1),
                        legendgroup=row.phone_id, showlegend=False
                    ),
                    row=1, col=i+1
                )

                # Bias point
                symbol = PHONE_SYMBOLS.get(row.phone_id, "circle")
                fig.add_trace(
                    go.Scatter(
                        x=[ox], y=[oy], mode='markers', name=f"{row.phone_id}",
                        legendgroup=row.phone_id, showlegend=(i==0),
                        marker=dict(
                            color=[row.global_time_num],
                            coloraxis='coloraxis',
                            symbol=symbol,
                            size=10,
                            line=dict(width=1, color='black')
                        )
                    ),
                    row=1, col=i+1
                )

            fig.update_xaxes(title_text=ox_col.replace("_offset", ""), row=1, col=i+1)
            fig.update_yaxes(title_text=oy_col.replace("_offset", ""), row=1, col=i+1)

        fig.update_layout(
            height=500, title_text="Accelerometer Calibration Parameters",
            coloraxis=dict(colorscale='Viridis', colorbar=dict(title="Time")),
            legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5)
        )
        return fig

    @output
    @render_plotly
    def cal_gyro_3d_plot():
        df = calibration_data()
        if df.empty:
            return go.Figure().update_layout(title="No data")

        fig = px.scatter_3d(
            df, x="RotVelX (rad/s)", y="RotVelY (rad/s)", z="RotVelZ (rad/s)",
            color="phone_id", opacity=0.5
        )
        fig.update_layout(title="Rotational Velocity 3D Scatter")
        return fig

    @output
    @render_plotly
    def cal_gyro_bias_plot():
        params = selected_parameters()
        if params.empty:
            return go.Figure().update_layout(title="No data")

        params = params.copy()
        params["global_time_num"] = _get_numeric_time(params)

        fig = px.scatter_3d(
            params, x="RotVelX (rad/s)_bias", y="RotVelY (rad/s)_bias", z="RotVelZ (rad/s)_bias",
            color="global_time_num", symbol="phone_id", 
            symbol_map=PHONE_SYMBOLS,
            text="global_time",
            color_continuous_scale="Viridis",
            labels={"global_time_num": "Time"},
            title="Rotational Velocity Bias"
        )
        fig.update_layout(
            legend=dict(orientation="h", yanchor="top", y=-0.05, xanchor="center", x=0.5)
        )
        return fig

    @output
    @render_plotly
    def cal_accel_x_plot():
        return _plot_dimension(calibration_data(), "LinAccX (m/s2)", "X (m/s2)")

    @output
    @render_plotly
    def cal_accel_y_plot():
        return _plot_dimension(calibration_data(), "LinAccY (m/s2)", "Y (m/s2)")

    @output
    @render_plotly
    def cal_accel_z_plot():
        return _plot_dimension(calibration_data(), "LinAccZ (m/s2)", "Z (m/s2)")

    @output
    @render_plotly
    def cal_gyro_x_plot():
        return _plot_dimension(calibration_data(), "RotVelX (rad/s)", "X (rad/s)")

    @output
    @render_plotly
    def cal_gyro_y_plot():
        return _plot_dimension(calibration_data(), "RotVelY (rad/s)", "Y (rad/s)")

    @output
    @render_plotly
    def cal_gyro_z_plot():
        return _plot_dimension(calibration_data(), "RotVelZ (rad/s)", "Z (rad/s)")
