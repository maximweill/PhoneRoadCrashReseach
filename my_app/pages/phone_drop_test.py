from pathlib import Path

import pandas as pd
import plotly.express as px
from shiny import reactive, render, ui
from shinywidgets import output_widget, render_plotly


DATA_DIR = Path(__file__).resolve().parents[1] / "data"
LOG_PATH = DATA_DIR / "lookup_tables_parquet" / "data_collection_log.parquet"
DROP_TEST_DIR = DATA_DIR / "phone_drop_test_data_parquet"
PHONE_FRAMED_DIR = DROP_TEST_DIR / "phone_framed"
REFERENCE_DIR = DROP_TEST_DIR / "phone_reference_signals"

TIME_COLUMN = "Time (s)"
SUMMARY_PLOT_HEIGHT = "720px"
COMPONENT_PLOT_HEIGHT = "680px"


def _read_log() -> pd.DataFrame:
    if not LOG_PATH.exists():
        return pd.DataFrame()

    df = pd.read_parquet(LOG_PATH)
    df = df[df["phone_id"].notna()].copy()
    if "Successful" in df.columns:
        df = df[df["Successful"]]

    df["target_speed_mps"] = df["target_speed_mps"].astype(int).astype(str)
    df["repeat"] = df["repeat"].astype(int).astype(str)
    return df


DATA_COLLECTION_LOG = _read_log()


def _sort_numeric_strings(values: pd.Series) -> list[str]:
    return sorted(values.dropna().astype(str).unique().tolist(), key=lambda value: int(value))


def _sorted_strings(values: pd.Series) -> list[str]:
    return sorted(values.dropna().astype(str).unique().tolist())


def _default_choice(choices: list[str], preferred: str) -> str | None:
    if preferred in choices:
        return preferred
    return choices[0] if choices else None


def _speed_choices() -> list[str]:
    if DATA_COLLECTION_LOG.empty:
        return []
    return _sort_numeric_strings(DATA_COLLECTION_LOG["target_speed_mps"])


def _config_choices() -> list[str]:
    if DATA_COLLECTION_LOG.empty:
        return []
    return _sorted_strings(DATA_COLLECTION_LOG["config"])


def _repeat_choices() -> list[str]:
    if DATA_COLLECTION_LOG.empty:
        return []
    return _sort_numeric_strings(DATA_COLLECTION_LOG["repeat"])


def _phone_choices() -> list[str]:
    if DATA_COLLECTION_LOG.empty:
        return ["All"]
    return ["All"] + _sorted_strings(DATA_COLLECTION_LOG["phone_id"])


def phone_drop_test_page():
    speeds = _speed_choices()
    configs = _config_choices()
    repeats = _repeat_choices()

    return ui.nav_panel(
        "Phone Drop Test Data",
        ui.layout_columns(
            ui.card(
                ui.card_header("Filters"),
                ui.input_select(
                    "drop_speed",
                    "Speed (m/s)",
                    choices=speeds,
                    selected=_default_choice(speeds, "6"),
                ),
                ui.input_select(
                    "drop_config",
                    "Config",
                    choices=configs,
                    selected=_default_choice(configs, "nYR"),
                ),
                ui.input_select(
                    "drop_repeat",
                    "Repeat",
                    choices=repeats,
                    selected=_default_choice(repeats, "1"),
                ),
                ui.input_select(
                    "drop_phone",
                    "Phone ID",
                    choices=_phone_choices(),
                    selected="All",
                ),
                fill=False,
            ),
            ui.card(
                ui.card_header("Processing Metadata"),
                ui.output_data_frame("drop_metadata_table"),
                fill=False,
            ),
            fill=False,
        ),
        ui.layout_columns(
            ui.card(
                ui.card_header("Accelerometer Comparison (m/s2)"),
                output_widget("drop_accel_plot", height=SUMMARY_PLOT_HEIGHT),
                full_screen=True,
                fill=False,
            ),
            ui.card(
                ui.card_header("Gyroscope Comparison (rad/s)"),
                output_widget("drop_gyro_plot", height=SUMMARY_PLOT_HEIGHT),
                full_screen=True,
                fill=False,
            ),
            ui.card(
                ui.card_header("Rotational Acceleration (rad/s2)"),
                output_widget("drop_rot_accel_res_plot", height=SUMMARY_PLOT_HEIGHT),
                full_screen=True,
                fill=False,
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
    )


def _component_card(title: str, output_id: str):
    return ui.card(
        ui.card_header(title),
        output_widget(output_id, height=COMPONENT_PLOT_HEIGHT),
        full_screen=True,
        fill=False,
    )


def _matching_rows(speed: str, config: str, repeat: str, phone: str) -> pd.DataFrame:
    df = DATA_COLLECTION_LOG.copy()
    if df.empty:
        return df

    df = df[
        (df["target_speed_mps"] == str(speed))
        & (df["config"] == config)
        & (df["repeat"] == str(repeat))
    ]
    if phone != "All":
        df = df[df["phone_id"] == phone]
    return df.sort_values("phone_id")


def _phone_sample_path(speed: str, config: str, repeat: str, phone_id: str) -> Path:
    return PHONE_FRAMED_DIR / f"{speed}mps_{config}_REPEAT{repeat}_{phone_id}_framed.parquet"


def _reference_sample_path(speed: str, config: str, repeat: str, phone_id: str) -> Path:
    return (
        REFERENCE_DIR
        / f"{speed}mps_{config}_REPEAT{repeat}_Headform_Transformed_{phone_id}_REF.parquet"
    )


def _read_sample(path: Path, source: str, phone_id: str) -> pd.DataFrame:
    if not path.exists():
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
    @reactive.effect
    def _update_repeat_choices():
        df = DATA_COLLECTION_LOG
        if df.empty:
            return

        speed = input.drop_speed()
        config = input.drop_config()
        matching = df[
            (df["target_speed_mps"] == str(speed))
            & (df["config"] == config)
        ]
        repeats = _sort_numeric_strings(matching["repeat"])
        current = input.drop_repeat()
        ui.update_select(
            "drop_repeat",
            choices=repeats,
            selected=current if current in repeats else _default_choice(repeats, "1"),
        )

    @reactive.effect
    def _update_phone_choices():
        df = DATA_COLLECTION_LOG
        if df.empty:
            return

        matching = df[
            (df["target_speed_mps"] == str(input.drop_speed()))
            & (df["config"] == input.drop_config())
            & (df["repeat"] == str(input.drop_repeat()))
        ]
        phones = ["All"] + _sorted_strings(matching["phone_id"])
        current = input.drop_phone()
        ui.update_select(
            "drop_phone",
            choices=phones,
            selected=current if current in phones else "All",
        )

    @reactive.calc
    def filtered_log():
        return _matching_rows(
            input.drop_speed(),
            input.drop_config(),
            input.drop_repeat(),
            input.drop_phone(),
        )

    @reactive.calc
    def drop_test_data():
        rows = filtered_log()
        frames = []

        for row in rows.itertuples(index=False):
            phone_id = row.phone_id
            speed = row.target_speed_mps
            config = row.config
            repeat = row.repeat

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
        columns = [
            "Date",
            "Test_Name",
            "phone_id",
            "config",
            "target_speed_mps",
            "repeat",
            "measured_speed_mps",
            "Successful",
            "Comments",
        ]
        available_columns = [column for column in columns if column in filtered_log().columns]
        return render.DataTable(filtered_log()[available_columns])

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
