from shiny.express import input, ui
from shiny import reactive, render
from shinywidgets import render_plotly
import plotly.express as px
from get_data import SAMPLE_CHOICES
import pandas as pd
import numpy as np
from get_data import devices_data,numeric_cols,manufacturers


#Crash Data -------------------------------------

@reactive.calc
def data():
    # Only load the columns needed for the main line plots + time
    name = input.file()
    path = SAMPLE_CHOICES[name]
    cols = ["time_s", "accelX_g", "accelY_g", "accelZ_g", 
            "gyroX_dps", "gyroY_dps", "gyroZ_dps", "time_ns"]
    return pd.read_parquet(path, columns=cols)

@reactive.calc
def sampling_rate():
    # We can pull just one column from the parquet for this calculation
    path = SAMPLE_CHOICES[input.file()]
    nano = pd.read_parquet(path, columns=["time_ns"])["time_ns"].to_numpy()
    differences_fps = 1 / (np.diff(nano) * 1e-9)
    return differences_fps

@reactive.calc
def accel_range():
    # Vectorized max is faster than list comprehension
    params = ["accelX_g", "accelY_g", "accelZ_g"]
    df = data()[params]
    return df.max().round(3).tolist()

@reactive.calc
def gyro_range():
    params = ["gyroX_dps", "gyroY_dps", "gyroZ_dps"]
    df = data()[params]
    return df.max().round(3).tolist()

# --- UI Definitions ---

with ui.nav_panel("Crash Data"):
    with ui.layout_columns():
        with ui.card(title="Filters"):
            ui.input_select("file", "Select Crash Record", choices=list(SAMPLE_CHOICES))
            
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
        def accel_plot():
            df = data()
            fig = px.line(
                df, x="time_s", y=["accelX_g", "accelY_g", "accelZ_g"],
                title="Accelerometer Data (g) vs Time (s)"
            )
            return fig

        @render_plotly
        def gyro_plot():
            df = data()
            fig = px.line(
                df, x="time_s", y=["gyroX_dps", "gyroY_dps", "gyroZ_dps"],
                title="Gyroscope Data (deg/s) vs Time (s)"
            )
            return fig

#Sensor Abilities ------------------------------------ 

@reactive.calc
def filtered_data():
    sub = devices_data[devices_data["manufacturer"] == input.manufacturer()]
    model_text = input.model_text().strip()
    if model_text:
        sub = sub[sub["model"].str.contains(model_text, case=False, na=False)]
    return sub

with ui.nav_panel("Sensor Abilities"):
    with ui.layout_columns():
        with ui.card(title="Filters", full_screen=False):
            ui.input_select("manufacturer", "Manufacturer", choices=manufacturers, selected="Apple")
            ui.input_text("model_text", "Text contained in Model Name", value="")
            ui.input_select("variable", "Variable", choices=numeric_cols, selected="accelerometer_rate")
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



with ui.nav_panel("About"):  
    ui.markdown(
        """
        **About this site**
        Data from Claire Baker: Standard car crashes and sled crashes.
        Processed via **Apache Parquet** for high-performance Imperial College research analysis.
        """
    )
    ui.markdown(
        """
        Phone sensor ability data comes from [phyphox sensordb](https://phyphox.org/sensordb).
        """
    )
    ui.markdown(
        """
        _This website was made by Maxim Weill._
        """
    )