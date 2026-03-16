from shiny.express import input, ui
from shiny import reactive, render
from shinywidgets import render_plotly
import plotly.express as px
import pandas as pd
import numpy as np
import helper
from get_data import (
    CRASH_SAMPLE_CHOICES,
    LOGS,
    LOGS_CHOICES,
    HEAD_DROP_SAMPLE_CHOICES,
    PHONE_DROP_SAMPLE_CHOICES,
    DEVICE_DATA_DIR_DATA,
    META_DATA_DIR
)



# Phone Drops Tests
@reactive.calc
def drop_test_log():
    name = input.test_name()
    # Force a KeyError if 'Test Name' doesn't exist or a filtered crash if empty
    filter_name = LOGS[LOGS["Test Name"] == name]
    if filter_name.empty:
        raise ValueError(f"CRASH: No data found for test name: '{name}'")
    return filter_name

@reactive.calc
def headform_data():
    log = drop_test_log()
    
    # 1. Get all filenames matching the filter
    headform_filenames = log[log["File Name"].str.contains("FILTERED")]["File Name"].tolist()
    
    # 2. Add a check to prevent IndexError/Crashing (similar to phone_data)
    if not headform_filenames:
        raise FileNotFoundError("No '_FILTERED' files found in the log for this test.")

    all_dfs = []
    for filename in headform_filenames:
        # 3. Lookup path and load
        # Note: Ensure filename exists in HEAD_DROP_SAMPLE_CHOICES
        path = HEAD_DROP_SAMPLE_CHOICES[filename]
        df = helper.load_head_data(path)
        
        # 4. Add the source_file column to match phone_data structure
        df["source_file"] = filename
        all_dfs.append(df)
    
    # 5. Return a single concatenated DataFrame
    return pd.concat(all_dfs, ignore_index=True)


@reactive.calc
def phone_data():
    log = drop_test_log()
    # Get filenames; if none contain 'crash_data', the loop won't run and concat will fail
    phone_filenames = log[log["File Name"].str.contains("crash_data")]["File Name"].tolist()
    
    if not phone_filenames:
        raise FileNotFoundError("CRASH: No 'crash_data' files found in the log for this test.")

    all_dfs = []
    for filename in phone_filenames:
        # Strict lookup: will crash if filename is missing from CHOICES
        path = PHONE_DROP_SAMPLE_CHOICES[filename]
        df = helper.load_phone_data(path)
        df["source_file"] = filename 
        all_dfs.append(df)
    
    # pd.concat will crash if all_dfs is empty (handled above, but good to know)
    return pd.concat(all_dfs, ignore_index=True)

@reactive.calc
def drop_data():
    return pd.concat([phone_data(), headform_data()], ignore_index=True)


with ui.nav_panel("Phone Drop Test Data"):
    with ui.layout_columns():
        with ui.card(title="Filters"):
            ui.input_select("test_name", "Select Drop Test", choices=sorted(list(LOGS_CHOICES)))
        with ui.card(title = "df summary"):
            @render.data_frame
            def df_head():
                # Displaying the first 10 rows of the selected test
                return render.DataTable(drop_test_log().head(10))
            
    with ui.card(title="Sensor Plots"):
        @render_plotly
        def multi_accel_plot():
            df = drop_data()

            # Melt will crash if any of these columns are missing from the CSVs
            df_long = df.melt(
                id_vars=["time_ns", "source_file"], 
                value_vars=["accelX_g", "accelY_g", "accelZ_g"],
                var_name="sensor_axis", 
                value_name="g_force"
            )

            labels = {"accelX_g": "Accel X", "accelY_g": "Accel Y", "accelZ_g": "Accel Z"}
            df_long["sensor_axis"] = df_long["sensor_axis"].map(labels)

            # Plotly Express will crash if df_long has unexpected types
            return px.line(
                df_long, 
                x="time_ns", 
                y="g_force", 
                facet_row="sensor_axis",
                color="source_file",
                labels={"g_force": "Acceleration (g)", "time_ns": "Time (ns)"},
                category_orders={"sensor_axis": ["Accel X", "Accel Y", "Accel Z"]}
            ).update_yaxes(matches=None)

        @render_plotly
        def multi_gyro_plot():
            df = drop_data()
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