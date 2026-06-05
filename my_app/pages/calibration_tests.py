from pathlib import Path
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from shiny import reactive, render, ui
from shinywidgets import output_widget, render_plotly
from scipy.stats import pearsonr

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
            # Row 1: Side-by-Side Plots
            ui.layout_columns(
                ui.card(
                    ui.card_header("Accelerometer Scale Factor vs. Prior Impact Speed"),
                    output_widget("cal_params_vs_speed_plot"),
                    full_screen=True,
                ),
                ui.card(
                    ui.card_header("Accelerometer Zero-Bias Offsets vs. Prior Impact Speed"),
                    output_widget("cal_bias_vs_speed_plot"),
                    full_screen=True,
                ),
                col_widths=[6, 6],
                fill=False,
            ),
            # Row 2: Full-width Summary Table
            ui.layout_columns(
                ui.card(
                    ui.card_header("Impact Sensitivity Regression Summary (Pearson r & p-values)"),
                    ui.output_table("cal_stats_summary_table"),
                    full_screen=True,
                ),
                col_widths=[12],
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

    @reactive.calc
    def cal_regression_metrics():
        """
        Calculates all linear regression (OLS) and Pearson statistics once 
        and shares the cached result with both plots and the summary table.
        """
        import pandas as pd
        params = selected_parameters()
        if params.empty or "measured_speed_mps" not in params.columns:
            return {}

        target_columns = [
            "LinAccX_scale", "LinAccY_scale", "LinAccZ_scale",
            "LinAccX_offset", "LinAccY_offset", "LinAccZ_offset"
        ]
        
        # Structure: {(phone_id, column_name): {metrics_dict}}
        metrics_cache = {}
        
        for phone_id in sorted(params["phone_id"].unique()):
            pdf = params[params["phone_id"] == phone_id]
            # Focus strictly on post-impact points (speed > 0)
            pdf_impacts = pdf[pdf["measured_speed_mps"] > 0]
            
            for col in target_columns:
                df_sub = pdf_impacts[[col, "measured_speed_mps"]].dropna()

                # Remove extreme outliers using 10 * MAD
                if len(df_sub) > 2:
                    median = df_sub[col].median()
                    mad = np.median(np.abs(df_sub[col] - median))

                    if mad > 0:
                        df_sub = df_sub[
                            np.abs(df_sub[col] - median) <= 10 * mad
                        ]

                # Default fallback values if data is sparse
                stats = {
                    "n": len(df_sub),
                    "r_val": None, "p_val": None,
                    "slope": None, "intercept": None,
                    "x_min": None, "x_max": None
                }

                if len(df_sub) > 2 and df_sub["measured_speed_mps"].nunique() > 1:
                    try:
                        # 1. Pearson Correlation
                        r_val, p_val = pearsonr(
                            df_sub["measured_speed_mps"],
                            df_sub[col]
                        )
                        stats["r_val"] = r_val
                        stats["p_val"] = p_val

                        # 2. OLS Trendline Fit
                        m, c = np.polyfit(
                            df_sub["measured_speed_mps"],
                            df_sub[col],
                            1
                        )
                        stats["slope"] = m
                        stats["intercept"] = c
                        stats["x_min"] = df_sub["measured_speed_mps"].min()
                        stats["x_max"] = df_sub["measured_speed_mps"].max()
                    except Exception:
                        pass

                metrics_cache[(phone_id, col)] = stats
                
        return metrics_cache

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
            height=500, 
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
            height=500,
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
    def cal_params_vs_speed_plot():
        params = selected_parameters()
        if params.empty:
            return go.Figure().update_layout(title="No data available")

        fig = make_subplots(
            rows=1, cols=3, 
            subplot_titles=("LinAccX Scale", "LinAccY Scale", "LinAccZ Scale")
        )
        
        scale_cols = ["LinAccX_scale", "LinAccY_scale", "LinAccZ_scale"]
        metrics = cal_regression_metrics()
        
        for i, col in enumerate(scale_cols):
            for phone_id in sorted(params["phone_id"].unique()):
                pdf = params[params["phone_id"] == phone_id]
                symbol = PHONE_SYMBOLS.get(phone_id, "circle")
                
                # Scatter points
                fig.add_trace(
                    go.Scatter(
                        x=pdf["measured_speed_mps"], y=pdf[col],
                        mode='markers', name=phone_id,
                        legendgroup=phone_id, showlegend=(i == 0),
                        marker=dict(symbol=symbol, size=7, opacity=0.8)
                    ),
                    row=1, col=i+1
                )
                
                # Retrieve OLS data from the reactive cache
                stats = metrics.get((phone_id, col), {})
                if stats.get("slope") is not None:
                    x_range = np.linspace(stats["x_min"], stats["x_max"], 100)
                    y_range = stats["slope"] * x_range + stats["intercept"]
                    fig.add_trace(
                        go.Scatter(
                            x=x_range, y=y_range,
                            mode='lines', name=f"{phone_id} Trend",
                            legendgroup=f"{phone_id}_trendline", showlegend=(i == 0),
                            line=dict(dash='dash', width=1.5),
                        ),
                        row=1, col=i+1
                    )
            
            fig.update_xaxes(title_text="Speed (m/s)", row=1, col=i+1)
            fig.update_yaxes(title_text="Scale Factor", row=1, col=i+1)
            
        fig.update_layout(
            height=380, margin=dict(t=40, b=40, l=40, r=40),
            legend=dict(orientation="h", yanchor="top", y=-0.25, xanchor="center", x=0.5)
        )
        return fig



    @output
    @render_plotly
    def cal_bias_vs_speed_plot():
        params = selected_parameters()
        if params.empty:
            return go.Figure().update_layout(title="No data available")

        fig = make_subplots(
            rows=1, cols=3, 
            subplot_titles=("LinAccX Offset", "LinAccY Offset", "LinAccZ Offset")
        )
        
        offset_cols = ["LinAccX_offset", "LinAccY_offset", "LinAccZ_offset"]
        metrics = cal_regression_metrics()
        
        for i, col in enumerate(offset_cols):
            for phone_id in sorted(params["phone_id"].unique()):
                pdf = params[params["phone_id"] == phone_id]
                symbol = PHONE_SYMBOLS.get(phone_id, "circle")
                
                # Scatter points
                fig.add_trace(
                    go.Scatter(
                        x=pdf["measured_speed_mps"], y=pdf[col],
                        mode='markers', name=phone_id,
                        legendgroup=phone_id, showlegend=(i == 0),
                        marker=dict(symbol=symbol, size=7, opacity=0.8)
                    ),
                    row=1, col=i+1
                )
                
                # Retrieve OLS data from the reactive cache
                stats = metrics.get((phone_id, col), {})
                if stats.get("slope") is not None:
                    x_range = np.linspace(stats["x_min"], stats["x_max"], 100)
                    y_range = stats["slope"] * x_range + stats["intercept"]
                    fig.add_trace(
                        go.Scatter(
                            x=x_range, y=y_range,
                            mode='lines', name=f"{phone_id} Trend",
                            legendgroup=f"{phone_id}_trendline", showlegend=(i == 0),
                            line=dict(dash='dash', width=1.5),
                        ),
                        row=1, col=i+1
                    )
            
            fig.update_xaxes(title_text="Speed (m/s)", row=1, col=i+1)
            fig.update_yaxes(title_text="Offset (m/s²)", row=1, col=i+1)
            
        fig.update_layout(
            height=380, margin=dict(t=40, b=40, l=40, r=40),
            legend=dict(orientation="h", yanchor="top", y=-0.25, xanchor="center", x=0.5)
        )
        return fig


    @output
    @render.table
    def cal_stats_summary_table():
        import pandas as pd
        params = selected_parameters()
        metrics = cal_regression_metrics()
        
        if not metrics:
            return pd.DataFrame({"Status": ["No data available to calculate metrics"]})
            
        target_columns = [
            "LinAccX_scale", "LinAccY_scale", "LinAccZ_scale",
            "LinAccX_offset", "LinAccY_offset", "LinAccZ_offset"
        ]
        
        records = []
        
        for phone_id in sorted(params["phone_id"].unique()):
            for col in target_columns:
                stats = metrics.get((phone_id, col), {})
                
                r_val = stats.get("r_val")
                p_val = stats.get("p_val")
                
                # Format string representations
                if r_val is not None:
                    r_str = f"{r_val:.3f}"
                    p_str = f"{p_val:.3e}" if p_val < 0.001 else f"{p_val:.4f}"
                    sig_flag = " * (Significant)" if p_val < 0.05 else ""
                else:
                    r_str, p_str, sig_flag = "N/A", "N/A", ""
                    
                param_display = col.replace("LinAcc", "Linear Accel ").replace("_", " ")
                
                records.append({
                    "Phone ID": phone_id,
                    "Calibration Parameter": param_display,
                    "Sample Size (N)": stats.get("n", 0),
                    "Pearson Correlation (r)": r_str,
                    "p-value": f"{p_str}{sig_flag}"
                })
                
        return pd.DataFrame(records)

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
