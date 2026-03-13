from shiny.express import input, ui
from shiny import reactive, render
from shinywidgets import render_plotly
import plotly.express as px
from get_data import (
    CRASH_SAMPLE_CHOICES,
    LOGS,
    LOGS_CHOICES,
    HEAD_DROP_SAMPLE_CHOICES,
    PHONE_DROP_SAMPLE_CHOICES,
    devices_data,
    numeric_cols,
    manufacturers
)
import pandas as pd
import numpy as np
import helper



# Phone Drops Tests
@reactive.calc
def drop_test_log():
    # Only load the columns needed for the main line plots + time
    name = input.test_name()
    filter_name = LOGS[LOGS["Test Name"] == name]
    return filter_name

@reactive.calc
def headform_data():
    log = drop_test_log()
    headform_filenames = log[log["File Name"].str.contains("_FILTERED")]
    head_path = HEAD_DROP_SAMPLE_CHOICES[headform_filenames[0]]
    return {headform_filenames[0]:helper.load_head_data(head_path)}

@reactive.calc
def phone_data():
    log = drop_test_log()
    phone_filenames = [log[log["File Name"].str.contains("crash_data")]["File Name"].tolist()[0]]
    
    all_dfs = []
    for filename in phone_filenames:
        df = helper.load_phone_data(PHONE_DROP_SAMPLE_CHOICES[filename])
        # Add a column to identify which file this data came from
        df["source_file"] = filename 
        all_dfs.append(df)
    
    # Combine all individual test dataframes into one
    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

with ui.nav_panel("Phone Drop Test Data"):
    with ui.layout_columns():
        with ui.card(title="Filters"):
            ui.input_select("test_name", "Select Drop Test", choices=list(LOGS_CHOICES))
            
    with ui.card(title="Sensor Plots"):
        @render_plotly
        def multi_accel_plot():
            df = phone_data()

            # 1. Melt the DataFrame to move axis names into a single column
            df_long = df.melt(
                id_vars=["time_ns", "source_file"], 
                value_vars=["accelX_g", "accelY_g", "accelZ_g"],
                var_name="sensor_axis", 
                value_name="g_force"
            )

            # 2. Map axis names to cleaner titles for the subplots
            labels = {
                "accelX_g": "Accel X",
                "accelY_g": "Accel Y",
                "accelZ_g": "Accel Z"
            }
            df_long["sensor_axis"] = df_long["sensor_axis"].map(labels)

            # 3. Create the plot
            fig = px.line(
                df_long, 
                x="time_ns", 
                y="g_force", 
                facet_row="sensor_axis", # Creates the 3 subplots (X, Y, Z)
                color="source_file",     # Keeps all phones on the same subplot per axis
                labels={"g_force": "Acceleration (g)", "time_ns": "Time (ns)", "source_file": "Phone ID"},
                category_orders={"sensor_axis": ["Accel X", "Accel Y", "Accel Z"]} # Ensures correct order
            )

            # 4. Clean up subplot titles and axes
            fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
            fig.update_yaxes(matches=None) # Allows each axis (X, Y, Z) to scale independently
            
            return fig

        @render_plotly
        def multi_gyro_plot():
            df = phone_data()

            df_long = df.melt(
                id_vars=["time_ns", "source_file"], 
                value_vars=["gyroX_dps", "gyroY_dps", "gyroZ_dps"],
                var_name="sensor_axis", 
                value_name="dps"
            )

            labels = {
                "gyroX_dps": "Gyro X",
                "gyroY_dps": "Gyro Y",
                "gyroZ_dps": "Gyro Z"
            }
            df_long["sensor_axis"] = df_long["sensor_axis"].map(labels)

            fig = px.line(
                df_long, 
                x="time_ns", 
                y="dps", 
                facet_row="sensor_axis",
                color="source_file",
                labels={"dps": "Degrees/s", "time_ns": "Time (ns)", "source_file": "Phone ID"},
                category_orders={"sensor_axis": ["Gyro X", "Gyro Y", "Gyro Z"]}
            )

            fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
            fig.update_yaxes(matches=None)
            
            return fig
        
#Reactives JLR -------------------------------------
@reactive.calc
def crash_data():
    # Only load the columns needed for the main line plots + time
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
        def accel_plot():
            df = crash_data()
            fig = px.line(
                df, x="time_ns", y=["accelX_g", "accelY_g", "accelZ_g"],
                title="Accelerometer Data (g) vs Time (s)"
            )
            return fig

        @render_plotly
        def gyro_plot():
            df = crash_data()
            fig = px.line(
                df, x="time_ns", y=["gyroX_dps", "gyroY_dps", "gyroZ_dps"],
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