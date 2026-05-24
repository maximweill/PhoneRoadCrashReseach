from pathlib import Path

import pandas as pd
import plotly.express as px
from shiny import reactive, render, ui
from shinywidgets import output_widget, render_plotly


DATA_DIR = Path(__file__).resolve().parents[1] / "data"
PHONE_CHARACTERISTICS_PATH = (
    DATA_DIR / "lookup_tables_parquet" / "phone_characteristics_aggregated.parquet"
)
START_CHARACTERISTICS_PATH = (
    DATA_DIR / "lookup_tables_parquet" / "start_characteristics_stationary.parquet"
)
END_CHARACTERISTICS_PATH = (
    DATA_DIR / "lookup_tables_parquet" / "end_characteristics_stationary.parquet"
)
STATIONARY_DIR = DATA_DIR / "stationary_parquet"
STATIONARY_START_PARSED = STATIONARY_DIR / "start" / "parsed"
STATIONARY_START_ALLAN = STATIONARY_DIR / "start" / "allan_variance"
STATIONARY_END_PARSED = STATIONARY_DIR / "end" / "parsed"
STATIONARY_END_ALLAN = STATIONARY_DIR / "end" / "allan_variance"

TIME_COLUMN = "Time (s)"
ACCEL_RES_COLUMN = "LinAccRes (m/s2)"
GYRO_RES_COLUMN = "RotVelRes (rad/s)"
TABLE_CARD_MIN_HEIGHT = "400px"
TRACE_PLOT_HEIGHT = "420px"
ALLAN_PLOT_HEIGHT = "460px"


def _get_all_phones() -> list[str]:
    phones = set()
    for directory in [STATIONARY_START_PARSED, STATIONARY_END_PARSED]:
        if directory.exists():
            for file in directory.glob("*.parquet"):
                # accel_stationary_20260511_001354_Phone004.parquet
                # both_stationary_20260520_125700_Phone003.parquet
                stem = file.stem
                parts = stem.split("_")
                if len(parts) >= 1:
                    phone_id = parts[-1]
                    if phone_id.startswith("Phone"):
                        phones.add(phone_id)
    return sorted(list(phones))


STATIONARY_PHONES = _get_all_phones()


def _default_stationary_phone() -> str | None:
    return STATIONARY_PHONES[0] if STATIONARY_PHONES else None


def tested_phone_characteristics_page():
    phone_choices = STATIONARY_PHONES or ["None"]

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
                ui.card_header("Start of Test Characteristics"),
                ui.output_data_frame("start_characteristics_table"),
                full_screen=True,
                min_height=TABLE_CARD_MIN_HEIGHT,
                fill=False,
            ),
            ui.card(
                ui.card_header("End of Test Characteristics"),
                ui.output_data_frame("end_characteristics_table"),
                full_screen=True,
                min_height=TABLE_CARD_MIN_HEIGHT,
                fill=False,
            ),
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
        ui.accordion(
            ui.accordion_panel(
                "Accelerometer - Start of Test",
                ui.layout_columns(
                    ui.card(
                        ui.card_header("Accel X (m/s2)"),
                        output_widget("accel_start_x_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                    ui.card(
                        ui.card_header("Accel Y (m/s2)"),
                        output_widget("accel_start_y_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                    ui.card(
                        ui.card_header("Accel Z (m/s2)"),
                        output_widget("accel_start_z_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                ),
                value="accel_start",
            ),
            ui.accordion_panel(
                "Accelerometer - End of Test",
                ui.layout_columns(
                    ui.card(
                        ui.card_header("Accel X (m/s2)"),
                        output_widget("accel_end_x_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                    ui.card(
                        ui.card_header("Accel Y (m/s2)"),
                        output_widget("accel_end_y_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                    ui.card(
                        ui.card_header("Accel Z (m/s2)"),
                        output_widget("accel_end_z_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                ),
                value="accel_end",
            ),
            ui.accordion_panel(
                "Gyroscope - Start of Test",
                ui.layout_columns(
                    ui.card(
                        ui.card_header("Gyro X (rad/s)"),
                        output_widget("gyro_start_x_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                    ui.card(
                        ui.card_header("Gyro Y (rad/s)"),
                        output_widget("gyro_start_y_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                    ui.card(
                        ui.card_header("Gyro Z (rad/s)"),
                        output_widget("gyro_start_z_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                ),
                value="gyro_start",
            ),
            ui.accordion_panel(
                "Gyroscope - End of Test",
                ui.layout_columns(
                    ui.card(
                        ui.card_header("Gyro X (rad/s)"),
                        output_widget("gyro_end_x_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                    ui.card(
                        ui.card_header("Gyro Y (rad/s)"),
                        output_widget("gyro_end_y_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                    ui.card(
                        ui.card_header("Gyro Z (rad/s)"),
                        output_widget("gyro_end_z_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                ),
                value="gyro_end",
            ),
            open="accel_start",
        ),
    )


def _load_sensor_data(phone_id: str | None, sensor: str, phase: str) -> pd.DataFrame:
    if not phone_id or phone_id == "None":
        return pd.DataFrame()

    directory = STATIONARY_START_PARSED if phase == "start" else STATIONARY_END_PARSED
    if not directory.exists():
        return pd.DataFrame()

    # In "end" phase, both sensors are in one file starting with "both"
    pattern = f"{sensor}_stationary_*_{phone_id}.parquet"
    if phase == "end":
        pattern = f"both_stationary_*_{phone_id}.parquet"

    files = list(directory.glob(pattern))
    frames = []
    for f in files:
        df = pd.read_parquet(f)
        if not df.empty:
            df["file"] = f.stem
            df["phase"] = phase.capitalize()
            frames.append(df)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _load_allan_data(phone_id: str | None, sensor: str) -> pd.DataFrame:
    if not phone_id or phone_id == "None":
        return pd.DataFrame()

    frames = []

    # Start phase
    if STATIONARY_START_ALLAN.exists():
        pattern = f"{sensor}_stationary_*_{phone_id}_allan.parquet"
        for f in STATIONARY_START_ALLAN.glob(pattern):
            df = pd.read_parquet(f)
            if not df.empty:
                df["file"] = f.stem
                df["phase"] = "Start"
                frames.append(df)

    # End phase
    if STATIONARY_END_ALLAN.exists():
        pattern = f"both_stationary_*_{phone_id}_allan.parquet"
        for f in STATIONARY_END_ALLAN.glob(pattern):
            df = pd.read_parquet(f)
            if not df.empty:
                df["file"] = f.stem
                df["phase"] = "End"
                frames.append(df)

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
        id_vars=["tau_s", "file", "phase"],
        value_vars=sigma_columns,
        var_name="component",
        value_name="sigma",
    )
    melted["component"] = melted["component"].str.replace("_sigma", "", regex=False)

    # Create a unique label for the legend
    melted["legend_label"] = melted["phase"] + " - " + melted["component"]

    fig = px.line(
        melted,
        x="tau_s",
        y="sigma",
        color="legend_label",
        line_dash="phase",
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
    def selected_accel_start_data():
        return _load_sensor_data(input.stationary_phone(), "accel", "start")

    @reactive.calc
    def selected_accel_end_data():
        return _load_sensor_data(input.stationary_phone(), "accel", "end")

    @reactive.calc
    def selected_gyro_start_data():
        return _load_sensor_data(input.stationary_phone(), "gyro", "start")

    @reactive.calc
    def selected_gyro_end_data():
        return _load_sensor_data(input.stationary_phone(), "gyro", "end")

    @reactive.calc
    def selected_allan_accel_data():
        return _load_allan_data(input.stationary_phone(), "accel")

    @reactive.calc
    def selected_allan_gyro_data():
        return _load_allan_data(input.stationary_phone(), "gyro")

    @output
    @render.data_frame
    def phone_characteristics_table():
        path = PHONE_CHARACTERISTICS_PATH
        if not path.exists():
            return render.DataTable(pd.DataFrame())
        return render.DataTable(pd.read_parquet(path))

    @output
    @render.data_frame
    def start_characteristics_table():
        path = START_CHARACTERISTICS_PATH
        if not path.exists():
            return render.DataTable(pd.DataFrame())
        return render.DataTable(pd.read_parquet(path))

    @output
    @render.data_frame
    def end_characteristics_table():
        path = END_CHARACTERISTICS_PATH
        if not path.exists():
            return render.DataTable(pd.DataFrame())
        return render.DataTable(pd.read_parquet(path))

    @output
    @render.text
    def stationary_stats():
        acc_s = selected_accel_start_data()
        acc_e = selected_accel_end_data()
        gyr_s = selected_gyro_start_data()
        gyr_e = selected_gyro_end_data()

        stats = []
        if not acc_s.empty and ACCEL_RES_COLUMN in acc_s.columns:
            stats.append(f"Accel Start RMS: {acc_s[ACCEL_RES_COLUMN].std():.4f} m/s2")
        if not acc_e.empty and ACCEL_RES_COLUMN in acc_e.columns:
            stats.append(f"Accel End RMS: {acc_e[ACCEL_RES_COLUMN].std():.4f} m/s2")
        if not gyr_s.empty and GYRO_RES_COLUMN in gyr_s.columns:
            stats.append(f"Gyro Start RMS: {gyr_s[GYRO_RES_COLUMN].std():.4f} rad/s")
        if not gyr_e.empty and GYRO_RES_COLUMN in gyr_e.columns:
            stats.append(f"Gyro End RMS: {gyr_e[GYRO_RES_COLUMN].std():.4f} rad/s")

        return " | ".join(stats) if stats else "No data selected."

    # Allan Plots
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

    # Accel Start Plots
    @output
    @render_plotly
    def accel_start_x_plot():
        return _stationary_line_plot(
            selected_accel_start_data(), "LinAccX (m/s2)", "Accel X - Start"
        )

    @output
    @render_plotly
    def accel_start_y_plot():
        return _stationary_line_plot(
            selected_accel_start_data(), "LinAccY (m/s2)", "Accel Y - Start"
        )

    @output
    @render_plotly
    def accel_start_z_plot():
        return _stationary_line_plot(
            selected_accel_start_data(), "LinAccZ (m/s2)", "Accel Z - Start"
        )

    # Accel End Plots
    @output
    @render_plotly
    def accel_end_x_plot():
        return _stationary_line_plot(
            selected_accel_end_data(), "LinAccX (m/s2)", "Accel X - End"
        )

    @output
    @render_plotly
    def accel_end_y_plot():
        return _stationary_line_plot(
            selected_accel_end_data(), "LinAccY (m/s2)", "Accel Y - End"
        )

    @output
    @render_plotly
    def accel_end_z_plot():
        return _stationary_line_plot(
            selected_accel_end_data(), "LinAccZ (m/s2)", "Accel Z - End"
        )

    # Gyro Start Plots
    @output
    @render_plotly
    def gyro_start_x_plot():
        return _stationary_line_plot(
            selected_gyro_start_data(), "RotVelX (rad/s)", "Gyro X - Start"
        )

    @output
    @render_plotly
    def gyro_start_y_plot():
        return _stationary_line_plot(
            selected_gyro_start_data(), "RotVelY (rad/s)", "Gyro Y - Start"
        )

    @output
    @render_plotly
    def gyro_start_z_plot():
        return _stationary_line_plot(
            selected_gyro_start_data(), "RotVelZ (rad/s)", "Gyro Z - Start"
        )

    # Gyro End Plots
    @output
    @render_plotly
    def gyro_end_x_plot():
        return _stationary_line_plot(
            selected_gyro_end_data(), "RotVelX (rad/s)", "Gyro X - End"
        )

    @output
    @render_plotly
    def gyro_end_y_plot():
        return _stationary_line_plot(
            selected_gyro_end_data(), "RotVelY (rad/s)", "Gyro Y - End"
        )

    @output
    @render_plotly
    def gyro_end_z_plot():
        return _stationary_line_plot(
            selected_gyro_end_data(), "RotVelZ (rad/s)", "Gyro Z - End"
        )
