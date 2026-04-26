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
    DEDUPLICATION_LOG,
    CROPPING_PARAMS,
    PHONE_DROP_SAMPLE_CHOICES,
    PHONE_REF_SAMPLE_CHOICES,
    DEVICE_DATA_DIR_DATA,
    META_DATA_DIR
)

# Phone Drop Tests --------------------------------------------------------
@reactive.calc
def drop_test_log():
    name = input.test_name()
    filter_name = LOGS[LOGS["Test Name"] == name]
    return filter_name

@reactive.calc
def drop_test_combined_data():
    log = drop_test_log()
    if log.empty:
        return pd.DataFrame()
    
    phone_filenames = log[log["File Name"].str.contains("crash_data", na=False)]["File Name"].tolist()
    
    all_dfs = []
    for filename in phone_filenames:
        # Ensure filename has .csv for matching if it doesn't already
        lookup_name = filename if filename.endswith(".csv") else f"{filename}.csv"
        stem = filename.replace(".csv", "")
        
        if stem in PHONE_DROP_SAMPLE_CHOICES:
            phone_id = stem.split("_")[-1]
            
            # Load Phone Data
            df_p = helper.load_phone_data(PHONE_DROP_SAMPLE_CHOICES[stem])
            df_p["source"] = "Phone"
            df_p["phone_id"] = phone_id
            df_p["file_stem"] = stem
            all_dfs.append(df_p)
            
            # Try to load Reference Data using lookup_name
            match = CROPPING_PARAMS[CROPPING_PARAMS["phone_file"] == lookup_name]
            if not match.empty:
                ref_filename = match.iloc[0]["ref_file"]
                # Clean up ref_stem to match PHONE_REF_SAMPLE_CHOICES keys
                ref_stem = ref_filename.replace(".csv", "").replace(".parquet", "")
                
                if ref_stem in PHONE_REF_SAMPLE_CHOICES:
                    df_r = helper.load_reference_data(PHONE_REF_SAMPLE_CHOICES[ref_stem])
                    df_r["source"] = "Reference"
                    df_r["phone_id"] = phone_id
                    df_r["file_stem"] = stem
                    all_dfs.append(df_r)
                    
    if not all_dfs:
        return pd.DataFrame()
        
    return pd.concat(all_dfs, ignore_index=True)

@reactive.calc
def drop_test_metadata_summary():
    log = drop_test_log()
    if log.empty:
        return pd.DataFrame()
    
    phone_filenames = log[log["File Name"].str.contains("crash_data", na=False)]["File Name"].tolist()
    summary = []
    
    for filename in phone_filenames:
        row = {"File": filename}
        
        # Add dedup info
        dedup_match = DEDUPLICATION_LOG[DEDUPLICATION_LOG["file"] == filename]
        if not dedup_match.empty:
            row["Removed"] = dedup_match.iloc[0]["removed"]
            row["% Removed"] = f"{dedup_match.iloc[0]['percent_removed']*100:.1f}%"
            
        # Add cropping info
        crop_match = CROPPING_PARAMS[CROPPING_PARAMS["phone_file"] == filename]
        if not crop_match.empty:
            row["Lag Index"] = crop_match.iloc[0]["lag_indices"]
            row["Ref Signal"] = crop_match.iloc[0]["ref_file"]
            
        summary.append(row)
        
    return pd.DataFrame(summary)

with ui.nav_panel("Phone Drop Test Data"):
    with ui.layout_columns():
        with ui.card(title="Filters"):
            ui.input_select("test_name", "Select Drop Test", choices=sorted(list(LOGS_CHOICES)))
        with ui.card(title = "Processing Metadata"):
            @render.data_frame
            def metadata_table():
                return render.DataTable(drop_test_metadata_summary())
            
    with ui.card(title="Accelerometer Magnitude Comparison"):
        @render_plotly
        def multi_accel_plot():
            df = drop_test_combined_data()
            if df.empty:
                return px.scatter(title="No data found for this test")
            
            # Using facet_row to separate phones
            fig = px.line(
                df, 
                x="time_ns", 
                y="accelMag_g", 
                color="source", 
                facet_row="phone_id",
                color_discrete_map={"Phone": "blue", "Reference": "red"},
                category_orders={"phone_id": sorted(df["phone_id"].unique())},
                labels={"accelMag_g": "Accel Mag (g)", "time_ns": "Time (ns)", "source": "Signal Type"},
                height=300 * len(df["phone_id"].unique())
            )
            fig.update_yaxes(matches=None)
            fig.for_each_annotation(lambda a: a.update(text=f"Phone {a.text.split('=')[-1]}"))
            return fig

    with ui.card(title="Gyroscope Magnitude Comparison"):
        @render_plotly
        def multi_gyro_plot():
            df = drop_test_combined_data()
            if df.empty:
                return px.scatter(title="No data found for this test")
                
            fig = px.line(
                df, 
                x="time_ns", 
                y="gyroMag_dps", 
                color="source", 
                facet_row="phone_id",
                color_discrete_map={"Phone": "blue", "Reference": "red"},
                category_orders={"phone_id": sorted(df["phone_id"].unique())},
                labels={"gyroMag_dps": "Gyro Mag (dps)", "time_ns": "Time (ns)", "source": "Signal Type"},
                height=300 * len(df["phone_id"].unique())
            )
            fig.update_yaxes(matches=None)
            fig.for_each_annotation(lambda a: a.update(text=f"Phone {a.text.split('=')[-1]}"))
            return fig

        
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
