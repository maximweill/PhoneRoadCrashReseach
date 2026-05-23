from pathlib import Path

import pandas as pd
import plotly.express as px
from shiny import reactive, render, ui
from shinywidgets import output_widget, render_plotly


DATA_DIR = Path(__file__).resolve().parents[1] / "data"
PHONE_CHARACTERISTICS_PATH = (
    DATA_DIR / "lookup_tables_parquet" / "phone_characteristics_aggregated.parquet"
)
STATIONARY_FRAMED_DIR = DATA_DIR / "stationary_parquet" / "parsed"
STATIONARY_ALLAN_DIR = DATA_DIR / "stationary_parquet" / "allan_variance"

TIME_COLUMN = "Time (s)"
ACCEL_RES_COLUMN = "LinAccRes (m/s2)"
GYRO_RES_COLUMN = "RotVelRes (rad/s)"
TABLE_CARD_MIN_HEIGHT = "400px"
TRACE_PLOT_HEIGHT = "420px"
ALLAN_PLOT_HEIGHT = "460px"


def _stationary_sample_choices() -> dict[str, Path]:
    if not STATIONARY_FRAMED_DIR.exists():
        return {}

    return {
        file.stem: file
        for file in STATIONARY_FRAMED_DIR.glob("*.parquet")
        if file.is_file() and file.stat().st_size > 0
    }


def _stationary_allan_choices() -> dict[str, Path]:
    if not STATIONARY_ALLAN_DIR.exists():
        return {}

    return {
        file.name: file
        for file in STATIONARY_ALLAN_DIR.glob("*.parquet")
        if file.is_file() and file.stat().st_size > 0
    }


def _get_stationary_phones() -> dict[str, dict[str, list[str]]]:
    phones: dict[str, dict[str, list[str]]] = {}

    for stem in _stationary_sample_choices():
        parts = stem.split("_")
        if len(parts) < 2:
            continue

        sensor = parts[0]
        phone_id = parts[-1]
        if sensor not in {"accel", "gyro"}:
            continue

        phones.setdefault(phone_id, {"accel": [], "gyro": []})
        phones[phone_id][sensor].append(stem)

    for phone in phones.values():
        phone["accel"].sort()
        phone["gyro"].sort()

    return dict(sorted(phones.items()))


STATIONARY_SAMPLE_CHOICES = _stationary_sample_choices()
STATIONARY_ALLAN_CHOICES = _stationary_allan_choices()
STATIONARY_PHONES = _get_stationary_phones()


def _default_stationary_phone() -> str | None:
    return next(iter(STATIONARY_PHONES), None)


def tested_phone_characteristics_page():
    phone_choices = list(STATIONARY_PHONES) or ["None"]

    return ui.nav_panel(
        "Tested Phone Characteristics",
        ui.card(
            ui.card_header("Aggregated Characteristics of Tested Phones"),
            ui.output_data_frame("phone_characteristics_table"),
            full_screen=True,
            min_height=TABLE_CARD_MIN_HEIGHT,
            fill=False,
        ),
        ui.layout_columns(
            ui.card(
                ui.card_header("Stationary Behavior Analysis"),
                ui.input_select(
                    "stationary_phone",
                    "Select Phone",
                    choices=phone_choices,
                    selected=_default_stationary_phone() or "None",
                ),
                fill=False,
            ),
            ui.card(
                ui.card_header("Stationary Noise Floor"),
                ui.output_text("stationary_stats"),
                fill=False,
            ),
            fill=False,
        ),
        ui.accordion(
            ui.accordion_panel(
                "Stationary Accelerometer Components",
                ui.layout_columns(
                    ui.card(
                        ui.card_header("Stationary Accelerometer X (m/s2)"),
                        output_widget(
                            "stationary_accel_x_plot",
                            height=TRACE_PLOT_HEIGHT,
                        ),
                        full_screen=True,
                        fill=False,
                    ),
                    ui.card(
                        ui.card_header("Stationary Accelerometer Y (m/s2)"),
                        output_widget(
                            "stationary_accel_y_plot",
                            height=TRACE_PLOT_HEIGHT,
                        ),
                        full_screen=True,
                        fill=False,
                    ),
                    ui.card(
                        ui.card_header("Stationary Accelerometer Z (m/s2)"),
                        output_widget(
                            "stationary_accel_z_plot",
                            height=TRACE_PLOT_HEIGHT,
                        ),
                        full_screen=True,
                        fill=False,
                    ),
                    fill=False,
                ),
                value="stationary_accelerometer",
            ),
            ui.accordion_panel(
                "Stationary Gyroscope Components",
                ui.layout_columns(
                    ui.card(
                        ui.card_header("Stationary Gyroscope X (rad/s)"),
                        output_widget(
                            "stationary_gyro_x_plot",
                            height=TRACE_PLOT_HEIGHT,
                        ),
                        full_screen=True,
                        fill=False,
                    ),
                    ui.card(
                        ui.card_header("Stationary Gyroscope Y (rad/s)"),
                        output_widget(
                            "stationary_gyro_y_plot",
                            height=TRACE_PLOT_HEIGHT,
                        ),
                        full_screen=True,
                        fill=False,
                    ),
                    ui.card(
                        ui.card_header("Stationary Gyroscope Z (rad/s)"),
                        output_widget(
                            "stationary_gyro_z_plot",
                            height=TRACE_PLOT_HEIGHT,
                        ),
                        full_screen=True,
                        fill=False,
                    ),
                    fill=False,
                ),
                value="stationary_gyroscope",
            ),
            ui.accordion_panel(
                "Allan Deviation",
                ui.layout_columns(
                    ui.card(
                        ui.card_header("Accelerometer Allan Deviation"),
                        output_widget(
                            "stationary_accel_allan_plot",
                            height=ALLAN_PLOT_HEIGHT,
                        ),
                        full_screen=True,
                        fill=False,
                    ),
                    ui.card(
                        ui.card_header("Gyroscope Allan Deviation"),
                        output_widget(
                            "stationary_gyro_allan_plot",
                            height=ALLAN_PLOT_HEIGHT,
                        ),
                        full_screen=True,
                        fill=False,
                    ),
                    fill=False,
                ),
                value="allan_deviation",
            ),
            open="stationary_accelerometer",
        ),
    )


def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _load_stationary_data(phone_id: str | None, sensor: str) -> pd.DataFrame:
    if not phone_id:
        return pd.DataFrame()

    stems = STATIONARY_PHONES.get(phone_id, {}).get(sensor, [])
    frames = []

    for stem in stems:
        path = STATIONARY_SAMPLE_CHOICES.get(stem)
        if path is None:
            continue

        frame = _read_parquet(path)
        if not frame.empty:
            frame = frame.copy()
            frame["file"] = stem
            frames.append(frame)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _load_allan_data(phone_id: str | None, sensor: str) -> pd.DataFrame:
    if not phone_id:
        return pd.DataFrame()

    stems = STATIONARY_PHONES.get(phone_id, {}).get(sensor, [])
    frames = []

    for stem in stems:
        path = STATIONARY_ALLAN_CHOICES.get(f"{stem}_allan.parquet")
        if path is None:
            continue

        frame = _read_parquet(path)
        if not frame.empty:
            frame = frame.copy()
            frame["file"] = stem
            frames.append(frame)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _empty_plot(title: str):
    return px.scatter(title=title)


def _stationary_line_plot(df: pd.DataFrame, column: str, title: str):
    if df.empty:
        return _empty_plot("No data")
    if TIME_COLUMN not in df.columns or column not in df.columns:
        return _empty_plot(f"Column not found: {column}")

    fig = px.line(df, x=TIME_COLUMN, y=column, color="file", title=title)
    fig.update_layout(
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.2,
            xanchor="center",
            x=0.5,
        )
    )
    return fig


def _allan_plot(df: pd.DataFrame, component_prefix: str, title: str):
    if df.empty:
        return _empty_plot("No Allan data")
    if "tau_s" not in df.columns:
        return _empty_plot("Column not found: tau_s")

    sigma_columns = [
        column
        for column in df.columns
        if column.startswith(component_prefix) and column.endswith("_sigma")
    ]
    if not sigma_columns:
        return _empty_plot(f"No {title.lower()} columns")

    melted = df.melt(
        id_vars=["tau_s", "file"],
        value_vars=sigma_columns,
        var_name="component",
        value_name="sigma",
    )
    melted["component"] = melted["component"].str.replace("_sigma", "", regex=False)

    fig = px.line(
        melted,
        x="tau_s",
        y="sigma",
        color="file",
        line_dash="component",
        title=title,
    )
    fig.update_xaxes(type="log")
    fig.update_yaxes(type="log")
    fig.update_layout(
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.2,
            xanchor="center",
            x=0.5,
        )
    )
    return fig


def register_tested_phone_characteristics_server(input, output, session):
    @reactive.calc
    def selected_stationary_accel_data():
        return _load_stationary_data(input.stationary_phone(), "accel")

    @reactive.calc
    def selected_stationary_gyro_data():
        return _load_stationary_data(input.stationary_phone(), "gyro")

    @reactive.calc
    def selected_allan_accel_data():
        return _load_allan_data(input.stationary_phone(), "accel")

    @reactive.calc
    def selected_allan_gyro_data():
        return _load_allan_data(input.stationary_phone(), "gyro")

    @output
    @render.data_frame
    def phone_characteristics_table():
        return render.DataTable(_read_parquet(PHONE_CHARACTERISTICS_PATH))

    @output
    @render.text
    def stationary_stats():
        accel_data = selected_stationary_accel_data()
        gyro_data = selected_stationary_gyro_data()

        stats = []
        if not accel_data.empty and ACCEL_RES_COLUMN in accel_data.columns:
            stats.append(
                f"Accel Noise (RMS): {accel_data[ACCEL_RES_COLUMN].std():.4f} m/s2"
            )
        if not gyro_data.empty and GYRO_RES_COLUMN in gyro_data.columns:
            stats.append(
                f"Gyro Noise (RMS): {gyro_data[GYRO_RES_COLUMN].std():.4f} rad/s"
            )

        return " | ".join(stats) if stats else "No data selected."

    @output
    @render_plotly
    def stationary_accel_x_plot():
        return _stationary_line_plot(
            selected_stationary_accel_data(),
            "LinAccX (m/s2)",
            "Accel X Noise",
        )

    @output
    @render_plotly
    def stationary_accel_y_plot():
        return _stationary_line_plot(
            selected_stationary_accel_data(),
            "LinAccY (m/s2)",
            "Accel Y Noise",
        )

    @output
    @render_plotly
    def stationary_accel_z_plot():
        return _stationary_line_plot(
            selected_stationary_accel_data(),
            "LinAccZ (m/s2)",
            "Accel Z Noise",
        )

    @output
    @render_plotly
    def stationary_gyro_x_plot():
        return _stationary_line_plot(
            selected_stationary_gyro_data(),
            "RotVelX (rad/s)",
            "Gyro X Noise",
        )

    @output
    @render_plotly
    def stationary_gyro_y_plot():
        return _stationary_line_plot(
            selected_stationary_gyro_data(),
            "RotVelY (rad/s)",
            "Gyro Y Noise",
        )

    @output
    @render_plotly
    def stationary_gyro_z_plot():
        return _stationary_line_plot(
            selected_stationary_gyro_data(),
            "RotVelZ (rad/s)",
            "Gyro Z Noise",
        )

    @output
    @render_plotly
    def stationary_accel_allan_plot():
        return _allan_plot(
            selected_allan_accel_data(),
            "LinAcc",
            "Accel Allan Deviation",
        )

    @output
    @render_plotly
    def stationary_gyro_allan_plot():
        return _allan_plot(
            selected_allan_gyro_data(),
            "RotVel",
            "Gyro Allan Deviation",
        )
