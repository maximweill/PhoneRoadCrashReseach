from shiny.express import input, ui
from shiny import reactive, render
from shinywidgets import render_plotly
import plotly.express as px
import pandas as pd
import numpy as np
import helper
from pathlib import Path
from get_data import (
    CRASH_SAMPLE_CHOICES,
    LOGS,
    LOGS_CHOICES,
    DEDUPLICATION_LOG,
    FRAMING_LOGS,
    PHONE_DROP_SAMPLE_CHOICES,
    PHONE_REF_SAMPLE_CHOICES,
    DEVICE_DATA_DIR_DATA,
    META_DATA_DIR,
    PHONE_CHARACTERISTICS_AGGREGATED
)

# Home --------------------------------------------------------
with ui.nav_panel("Home"):  
    ui.markdown(
        """
        # Phone Road Crash Research Portal
        Welcome to the research dashboard for investigating smartphone sensor performance in road crash scenarios.

        ### Data Modules
        * **Phone Drop Test Data**: Controlled laboratory tests conducted at Imperial College London.
        * **Crash Data**: Real-world and sled-based car crash records.
        * **Sensor Abilities**: Global smartphone hardware specifications from Phyphox SensorDB.

        ---
        _Developed by Maxim Weill_
        """
    )
# Tested Phone Characteristics -------------
with ui.nav_panel("Tested Phone Characteristics"):
    with ui.card(full_screen=True):
        ui.card_header("Aggregated Characteristics of Tested Phones")
        @render.data_frame
        def phone_characteristics_table():
            return render.DataTable(PHONE_CHARACTERISTICS_AGGREGATED)

# Phone Drop Tests --------------------------------------------------------
@reactive.calc
def filtered_logs():
    df = FRAMING_LOGS.copy()
    if input.speed() != "All": df = df[df["speed"] == input.speed()]
    if input.config() != "All": df = df[df["config"] == input.config()]
    if input.repeat() != "All": df = df[df["repeat"] == input.repeat()]
    if input.phone() != "All": df = df[df["phone_id"] == input.phone()]
    return df

@reactive.calc
def drop_test_data():
    logs = filtered_logs()
    if logs.empty: return pd.DataFrame()
    
    all_dfs = []
    for _, row in logs.iterrows():
        p_stem = Path(row["framed_file"]).stem
        r_stem = Path(row["ref_file"]).stem
        
        if p_stem in PHONE_DROP_SAMPLE_CHOICES:
            df_p = helper.load_phone_data(PHONE_DROP_SAMPLE_CHOICES[p_stem])
            df_p["source"], df_p["phone_id"] = "Phone", row["phone_id"]
            df_p["file"] = p_stem
            all_dfs.append(df_p)
            
        if r_stem in PHONE_REF_SAMPLE_CHOICES:
            df_r = helper.load_reference_data(PHONE_REF_SAMPLE_CHOICES[r_stem])
            df_r["source"], df_r["phone_id"] = "Reference", row["phone_id"]
            df_r["file"] = r_stem
            all_dfs.append(df_r)
            
    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

def plot_component(col):
    df = drop_test_data()
    if df.empty: return px.scatter(title="No data")
    return px.line(
        df, x="Time (s)", y=col, 
        color="source", 
        line_dash="source",
        color_discrete_map={"Reference": "rgba(255, 0, 0, 0.3)", "Phone": "rgba(0, 0, 255, 0.6)"},
        line_dash_map={"Reference": "dash", "Phone": "solid"},
        facet_row="phone_id", 
        line_group="file"
    )

with ui.nav_panel("Phone Drop Test Data"):
    with ui.layout_columns():
        with ui.card(title="Filters"):
            ui.input_select("speed", "Speed", choices=["All"] + sorted(FRAMING_LOGS["speed"].unique().tolist()), selected="6mps")
            ui.input_select("config", "Config", choices=["All"] + sorted(FRAMING_LOGS["config"].unique().tolist()), selected="nYR")
            ui.input_select("repeat", "Repeat", choices=["All"] + sorted(FRAMING_LOGS["repeat"].unique().tolist()), selected="REPEAT1")
            ui.input_select("phone", "Phone ID", choices=["All"] + sorted(FRAMING_LOGS["phone_id"].unique().tolist()), selected="All")
        
        with ui.card(title="Processing Metadata"):
            @render.data_frame
            def metadata_table():
                return render.DataTable(filtered_logs()[["phone_id", "lag", "offset"]])
            
    with ui.layout_columns():
        with ui.card(title="Accelerometer Comparison (m/s2)"):
            ui.card_header("Accelerometer Comparison (m/s2)")
            @render_plotly
            def accel_plot():
                return plot_component("LinAccRes (m/s2)")

        with ui.card(title="Gyroscope Comparison (rad/s)"):
            ui.card_header("Gyroscope Comparison (rad/s)")
            @render_plotly
            def gyro_plot():
                return plot_component("RotVelRes (rad/s)")
        
        with ui.card(title="Rotational Acceleration (rad/s2)"):
            ui.card_header("Rotational Acceleration (rad/s2)")
            @render_plotly
            def rot_accel_res_plot():
                return plot_component("RotAccRes (rad/s2)")

    with ui.accordion(open=False):
        with ui.accordion_panel("Linear Acceleration XYZ Components"):
            with ui.layout_columns():
                with ui.card():
                    ui.card_header("LinAcc X")
                    @render_plotly
                    def plot_accel_x(): return plot_component("LinAccX (m/s2)")
                with ui.card():
                    ui.card_header("LinAcc Y")
                    @render_plotly
                    def plot_accel_y(): return plot_component("LinAccY (m/s2)")
                with ui.card():
                    ui.card_header("LinAcc Z")
                    @render_plotly
                    def plot_accel_z(): return plot_component("LinAccZ (m/s2)")

        with ui.accordion_panel("Rotational Velocity XYZ Components"):
            with ui.layout_columns():
                with ui.card():
                    ui.card_header("RotVel X")
                    @render_plotly
                    def plot_gyro_x(): return plot_component("RotVelX (rad/s)")
                with ui.card():
                    ui.card_header("RotVel Y")
                    @render_plotly
                    def plot_gyro_y(): return plot_component("RotVelY (rad/s)")
                with ui.card():
                    ui.card_header("RotVel Z")
                    @render_plotly
                    def plot_gyro_z(): return plot_component("RotVelZ (rad/s)")

        with ui.accordion_panel("Rotational Acceleration XYZ Components"):
            with ui.layout_columns():
                with ui.card():
                    ui.card_header("RotAcc X")
                    @render_plotly
                    def plot_rotacc_x(): return plot_component("RotAccX (rad/s2)")
                with ui.card():
                    ui.card_header("RotAcc Y")
                    @render_plotly
                    def plot_rotacc_y(): return plot_component("RotAccY (rad/s2)")
                with ui.card():
                    ui.card_header("RotAcc Z")
                    @render_plotly
                    def plot_rotacc_z(): return plot_component("RotAccZ (rad/s2)")

# Crash Data -------------------------------------
@reactive.calc
def crash_data():
    name = input.file()
    path = CRASH_SAMPLE_CHOICES[name]
    return helper.load_phone_data(path=path)

@reactive.calc
def sampling_rate():
    return helper.get_phone_sampling_rate(crash_data())

@reactive.calc
def accel_range():
    return helper.get_peak_accel(crash_data())

@reactive.calc
def gyro_range():
    return helper.get_peak_gyro(crash_data())

with ui.nav_panel("Crash Data"):
    with ui.layout_columns():
        with ui.card(title="Filters"):
            ui.input_select("file", "Select Crash Record", choices=list(CRASH_SAMPLE_CHOICES))
            
        with ui.card(title="MetaData"):
            @render.text
            def accel_range_txt():
                return f"Accelerometer max g (X, Y, Z): {accel_range()}"
                
            @render.text
            def gyro_range_txt():
                return f"Gyroscope max deg/s (X, Y, Z): {gyro_range()}"

    with ui.card(title="Sampling Rate Analysis"):
        @render_plotly
        def boxplot():
            lst = sampling_rate()
            fig = px.box(y=lst, title="Distribution of Sampling Rate (Hz)")
            fig.update_layout(yaxis_title="Hz", yaxis_range=[200, 800])
            return fig

    with ui.card(title="Sensor Plots"):
        @render_plotly
        def accel_plot_crash():
            df = crash_data()
            fig = px.line(
                df, x="time_ns", y=["accelX_g", "accelY_g", "accelZ_g"],
                title="Accelerometer Data (g) vs Time (s)"
            )
            return fig

        @render_plotly
        def gyro_plot_crash():
            df = crash_data()
            fig = px.line(
                df, x="time_ns", y=["gyroX_dps", "gyroY_dps", "gyroZ_dps"],
                title="Gyroscope Data (deg/s) vs Time (s)"
            )
            return fig

# Sensor Abilities ------------------------------------ 


@reactive.calc
def devices_data():
    return pd.read_parquet(DEVICE_DATA_DIR_DATA, engine="pyarrow")
 
@reactive.calc
def meta_df():
    return pd.read_parquet(META_DATA_DIR, engine="pyarrow")

@reactive.calc
def manufacturers():
    cols = meta_df()["manufacturers"].iloc[0].tolist() 
    return sorted(cols, key=lambda s: str(s).lower())

@reactive.calc
def numeric_cols():
    cols = meta_df()["numeric_cols"].iloc[0].tolist()
    return sorted(cols)


@reactive.calc
def filtered_data():
    sub = devices_data()[devices_data()["manufacturer"] == input.manufacturer()]
    model_text = input.model_text().strip()
    if model_text:
        sub = sub[sub["model"].str.contains(model_text, case=False, na=False)]
    return sub

@reactive.effect
def _():
    # Update manufacturer choices
    m_choices = manufacturers()
    ui.update_select("manufacturer", choices=m_choices, selected="Apple")
    
    # Update variable choices
    v_choices = numeric_cols()
    ui.update_select("variable", choices=v_choices, selected="accelerometer_rate")

with ui.nav_panel("Sensor Abilities"):
    with ui.layout_columns():
        with ui.card(title="Filters", full_screen=False):
            ui.input_select("manufacturer", "Manufacturer", choices=[], selected=None)
            ui.input_text("model_text", "Text contained in Model Name", value="")
            ui.input_select("variable", "Variable", choices=[], selected=None)
        with ui.card(title="Distribution", full_screen=False):
            @render_plotly
            def boxplot2():
                sub = filtered_data()
                var = input.variable()

                if sub.empty:
                    ui.notification_show("No data available.", duration=2)
                    return
                if var not in sub.columns:
                    ui.notification_show(f"Column '{var}' not found in data.", duration=2)
                    return
                if sub[var].dropna().empty:
                    ui.notification_show(f"No valid data for '{var}'.", duration=2)
                    return
                
                fig = px.box(
                    sub,
                    y=var
                ).update_layout(
                    title=f"Distribution of {var.replace('_', ' ').title()}",
                )
                return fig
        with ui.card(title="Availability", full_screen=False):
            @render_plotly
            def pie_chart():
                sub = filtered_data()
                var = input.variable()
                name_col = "_".join(var.split('_')[:-1]+["available"])

                if sub.empty:
                    ui.notification_show("No data available.", duration=None)
                    return
                if name_col not in sub.columns:
                    ui.notification_show(name_col + " not found in data.", duration=2)
                    return

                # Count True/False
                counts = sub[name_col].value_counts().reset_index()
                counts.columns = [name_col, "count"]

                # Map True/False to colors
                color_map = {
                    True: "blue",
                    False: "red"
                }
                tt_txt = " ".join(var.split('_')[:-1]).title()

                fig = px.pie(
                    counts,
                    names=name_col,
                    values="count",
                    color=name_col,
                    color_discrete_map=color_map,
                ).update_layout(
                    title="Availability of " + tt_txt,
                )
                return fig

    with ui.card(title="Histogram", full_screen=True):
        @render_plotly
        def histogram():
            sub = filtered_data()
            var = input.variable()
            ordered = sub.sort_values(var)
            fig = px.bar(
                ordered,
                x="model",
                y=var
            ).update_layout(
                title="Ordered Histogram by Model",
                xaxis_title="Model",
                yaxis_title=var
            )
            return fig

