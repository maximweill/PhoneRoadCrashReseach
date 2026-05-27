from pathlib import Path

import pandas as pd
import plotly.express as px
from shiny import reactive, render, ui
from shinywidgets import output_widget, render_plotly

from .standard_filter import (
    DROP_INDEX,
    DATA_DIR,
    filter_log_by_input,
    get_phone_card,
    get_speed_cards,
    get_unique_drops,
    sort_numeric_strings,
    sorted_strings,
)

DATA_COLLECTION_LOG_PATH = DATA_DIR / "lookup_tables_parquet" / "data_collection_log.parquet"

TIME_COLUMN = "Time (s)"
SUMMARY_PLOT_HEIGHT = "100%"
COMPONENT_PLOT_HEIGHT = "100%"

def load_data_collection_log() -> pd.DataFrame:
    if not DATA_COLLECTION_LOG_PATH.exists():
        return pd.DataFrame()
    df = pd.read_parquet(DATA_COLLECTION_LOG_PATH)
    # Ensure types match for merging
    df["target_speed_mps_str"] = df["target_speed_mps"].astype(str)
    df["repeat_str"] = df["repeat"].astype(str)
    return df

DATA_COLLECTION_LOG = load_data_collection_log()

def phone_drop_test_page():
    unique_drops = get_unique_drops()
    if unique_drops.empty:
        return ui.nav_panel("Phone Drop Test Data", ui.p("No data available"))

    speed_cards = get_speed_cards("drop")

    return ui.nav_panel(
        "Phone Drop Test Data",
        ui.layout_sidebar(
            ui.sidebar(
                ui.input_action_button(
                    "update_drop_plots",
                    "Update Plots",
                    class_="btn-primary w-100",
                ),
                ui.hr(),
                get_phone_card("drop"),
                *speed_cards,
                width=350,
            ),
            ui.layout_columns(
                ui.card(
                    ui.card_header("Accelerometer Comparison (m/s2)"),
                    output_widget("drop_accel_plot", height=SUMMARY_PLOT_HEIGHT),
                    full_screen=True,
                    fill=True,
                ),
                ui.card(
                    ui.card_header("Gyroscope Comparison (rad/s)"),
                    output_widget("drop_gyro_plot", height=SUMMARY_PLOT_HEIGHT),
                    full_screen=True,
                    fill=True,
                ),
                ui.card(
                    ui.card_header("Rotational Acceleration (rad/s2)"),
                    output_widget("drop_rot_accel_res_plot", height=SUMMARY_PLOT_HEIGHT),
                    full_screen=True,
                    fill=True,
                ),
                fill=False,
            ),
            ui.accordion(
                ui.accordion_panel(
                    "Linear Acceleration XYZ Components",
                    ui.layout_columns(
                        _component_card("LinAcc X", "drop_accel_x_plot"),
                        _component_card("LinAcc Y", "drop_accel_y_plot"),
                        _component_card("LinAcc Z", "drop_accel_z_plot"),
                        fill=False,
                    ),
                    value="drop_linear_acceleration",
                ),
                ui.accordion_panel(
                    "Rotational Velocity XYZ Components",
                    ui.layout_columns(
                        _component_card("RotVel X", "drop_gyro_x_plot"),
                        _component_card("RotVel Y", "drop_gyro_y_plot"),
                        _component_card("RotVel Z", "drop_gyro_z_plot"),
                        fill=False,
                    ),
                    value="drop_rotational_velocity",
                ),
                ui.accordion_panel(
                    "Rotational Acceleration XYZ Components",
                    ui.layout_columns(
                        _component_card("RotAcc X", "drop_rotacc_x_plot"),
                        _component_card("RotAcc Y", "drop_rotacc_y_plot"),
                        _component_card("RotAcc Z", "drop_rotacc_z_plot"),
                        fill=False,
                    ),
                    value="drop_rotational_acceleration",
                ),
                open=False,
            ),
            ui.card(
                ui.card_header("Processing Metadata"),
                ui.output_data_frame("drop_metadata_table"),
                fill=False,
            ),
        ),
    )


def _component_card(title: str, output_id: str):
    return ui.card(
        ui.card_header(title),
        output_widget(output_id, height=COMPONENT_PLOT_HEIGHT),
        full_screen=True,
        fill=False,
    )


def _phone_sample_path(speed: str | int, config: str, repeat: str | int, phone_id: str) -> Path | None:
    if DROP_INDEX.empty:
        return None
    
    # Cast incoming UI strings to match the dataframe's native integers
    mask = (
        (DROP_INDEX["target_speed_mps"] == int(speed)) &
        (DROP_INDEX["config"] == config) &
        (DROP_INDEX["repeat"] == int(repeat)) &
        (DROP_INDEX["phone_id"] == phone_id) &
        (DROP_INDEX["data"] == "framed")  # Matches your pipeline's string 'data_type'
    )
    res = DROP_INDEX[mask]["path"]
    if not res.empty:
        return DATA_DIR / res.iloc[0]
    return None


def _reference_sample_path(speed: str, config: str, repeat: str, phone_id: str) -> Path | None:
    if DROP_INDEX.empty:
        return None

    mask = (
        (DROP_INDEX["target_speed_mps_str"] == str(speed)) &
        (DROP_INDEX["config"] == config) &
        (DROP_INDEX["repeat_str"] == str(repeat)) &
        (DROP_INDEX["phone_id"] == phone_id) &
        (DROP_INDEX["data"] == "reference")
    )
    res = DROP_INDEX[mask]["path"]
    if not res.empty:
        return DATA_DIR / res.iloc[0]
    return None


def _read_sample(path: Path | None, source: str, phone_id: str) -> pd.DataFrame:
    if path is None or not path.exists():
        return pd.DataFrame()

    df = pd.read_parquet(path)
    df = df.copy()
    df["source"] = source
    df["phone_id"] = phone_id
    df["file"] = path.stem
    return df


def _empty_plot(title: str):
    return px.scatter(title=title)


def _plot_component(df: pd.DataFrame, column: str):
    if df.empty:
        return _empty_plot("No data")
    if TIME_COLUMN not in df.columns or column not in df.columns:
        return _empty_plot(f"Column not found: {column}")

    fig = px.line(
        df,
        x=TIME_COLUMN,
        y=column,
        color="source",
        line_dash="source",
        facet_row="phone_id",
        line_group="file",
        color_discrete_map={
            "Reference": "rgba(210, 45, 45, 0.55)",
            "Phone": "rgba(35, 95, 180, 0.75)",
        },
        line_dash_map={"Reference": "dash", "Phone": "solid"},
    )
    fig.update_layout(
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.2,
            xanchor="center",
            x=0.5,
        ),
        margin=dict(t=40, r=20, b=80, l=60),
    )
    return fig


def register_phone_drop_test_server(input, output, session):
    @reactive.calc
    @reactive.event(input.update_drop_plots, ignore_none=False)
    def filtered_log():
        return filter_log_by_input(input, "drop", "drop_phone_id")

    @reactive.calc
    def drop_test_data():
        rows = filtered_log()
        frames = []

        for row in rows.itertuples(index=False):
            phone_id = row.phone_id
            speed = row.target_speed_mps_str
            config = row.config
            repeat = row.repeat_str

            phone_data = _read_sample(
                _phone_sample_path(speed, config, repeat, phone_id),
                "Phone",
                phone_id,
            )
            reference_data = _read_sample(
                _reference_sample_path(speed, config, repeat, phone_id),
                "Reference",
                phone_id,
            )
            if not phone_data.empty:
                frames.append(phone_data)
            if not reference_data.empty:
                frames.append(reference_data)

        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    @output
    @render.data_frame
    def drop_metadata_table():
        if DATA_COLLECTION_LOG.empty:
            return render.DataTable(pd.DataFrame())
        
        columns = [
            "Date",
            "phone_id",
            "config",
            "target_speed_mps",
            "repeat",
            "measured_speed_mps",
            "Successful",
            "Comments",
        ]
        available_columns = [column for column in columns if column in DATA_COLLECTION_LOG.columns]
        return render.DataTable(DATA_COLLECTION_LOG[available_columns])

    @output
    @render_plotly
    def drop_accel_plot():
        return _plot_component(drop_test_data(), "LinAccRes (m/s2)")

    @output
    @render_plotly
    def drop_gyro_plot():
        return _plot_component(drop_test_data(), "RotVelRes (rad/s)")

    @output
    @render_plotly
    def drop_rot_accel_res_plot():
        return _plot_component(drop_test_data(), "RotAccRes (rad/s2)")

    @output
    @render_plotly
    def drop_accel_x_plot():
        return _plot_component(drop_test_data(), "LinAccX (m/s2)")

    @output
    @render_plotly
    def drop_accel_y_plot():
        return _plot_component(drop_test_data(), "LinAccY (m/s2)")

    @output
    @render_plotly
    def drop_accel_z_plot():
        return _plot_component(drop_test_data(), "LinAccZ (m/s2)")

    @output
    @render_plotly
    def drop_gyro_x_plot():
        return _plot_component(drop_test_data(), "RotVelX (rad/s)")

    @output
    @render_plotly
    def drop_gyro_y_plot():
        return _plot_component(drop_test_data(), "RotVelY (rad/s)")

    @output
    @render_plotly
    def drop_gyro_z_plot():
        return _plot_component(drop_test_data(), "RotVelZ (rad/s)")

    @output
    @render_plotly
    def drop_rotacc_x_plot():
        return _plot_component(drop_test_data(), "RotAccX (rad/s2)")

    @output
    @render_plotly
    def drop_rotacc_y_plot():
        return _plot_component(drop_test_data(), "RotAccY (rad/s2)")

    @output
    @render_plotly
    def drop_rotacc_z_plot():
        return _plot_component(drop_test_data(), "RotAccZ (rad/s2)")
