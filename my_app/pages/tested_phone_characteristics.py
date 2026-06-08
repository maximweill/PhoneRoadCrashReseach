import re
from pathlib import Path

import pandas as pd
import plotly.express as px
from shiny import reactive, render, ui
from shinywidgets import output_widget, render_plotly


PAGES_DIR = Path(__file__).resolve().parent
APP_DIR = PAGES_DIR.parent
DATA_DIR = APP_DIR / "data"
PHONE_CHARACTERISTICS_PATH = (
    DATA_DIR / "lookup_tables_parquet" / "characteristics" / "phone_characteristics_aggregated.parquet"
)
START_CHARACTERISTICS_PATH = (
    DATA_DIR / "lookup_tables_parquet" / "characteristics" / "start_characteristics_stationary.parquet"
)
END_CHARACTERISTICS_PATH = (
    DATA_DIR / "lookup_tables_parquet" / "characteristics" / "end_characteristics_stationary.parquet"
)
TIME_COLUMN = "Time (s)"
ACCEL_RES_COLUMN = "LinAccRes (m/s2)"
GYRO_RES_COLUMN = "RotVelRes (rad/s)"
TABLE_CARD_MIN_HEIGHT = "400px"
TRACE_PLOT_HEIGHT = "420px"
ALLAN_PLOT_HEIGHT = "460px"
PSD_PLOT_HEIGHT = "460px"


def _read_stationary_index() -> pd.DataFrame:
    stationary_dir = DATA_DIR / "stationary_parquet"
    if not stationary_dir.exists():
        return pd.DataFrame()

    files = []
    # Use rglob to find all parquet files in stationary_parquet
    for p in stationary_dir.rglob("*.parquet"):
        # p is absolute path
        # Example: .../data/stationary_parquet/start/parsed/accel_...parquet
        try:
            rel_to_stationary = p.relative_to(stationary_dir)
            parts = rel_to_stationary.parts
            if len(parts) < 2:
                continue

            session = parts[0]  # start, end
            data_type = parts[1]  # parsed, allan_variance, power_spectral_density

            filename = p.name
            match = re.search(r"(Phone\d+)", filename)
            if match:
                phone_id = match.group(1)
                # path relative to DATA_DIR
                rel_path = p.relative_to(DATA_DIR)
                files.append(
                    {
                        "phone_id": phone_id,
                        "session": session,
                        "type": data_type,
                        "file_name": filename,
                        "path": rel_path.as_posix(),
                    }
                )
        except ValueError:
            continue

    return pd.DataFrame(files)


STATIONARY_INDEX = _read_stationary_index()


def _get_all_phones() -> list[str]:
    if STATIONARY_INDEX.empty:
        return []
    return sorted(STATIONARY_INDEX["phone_id"].unique().tolist())


STATIONARY_PHONES = _get_all_phones()


def _default_stationary_phone() -> str | None:
    if "Phone002" in STATIONARY_PHONES:
        return "Phone002"
    return STATIONARY_PHONES[-1] if STATIONARY_PHONES else None


def tested_phone_characteristics_page():
    phone_choices = STATIONARY_PHONES or ["None"]

    return ui.nav_panel(
        "Tested Phone Characteristics",
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
                ui.card_header("Accelerometer Power Spectral Density"),
                output_widget(
                    "stationary_accel_psd_plot",
                    height=PSD_PLOT_HEIGHT,
                ),
                full_screen=True,
                fill=False,
            ),
            ui.card(
                ui.card_header("Gyroscope Power Spectral Density"),
                output_widget(
                    "stationary_gyro_psd_plot",
                    height=PSD_PLOT_HEIGHT,
                ),
                full_screen=True,
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
    )

def _get_stem(x):
    return Path(x).stem if isinstance(x, str) else x
def _load_sensor_data(phone_id: str | None, sensor: str, phase: str) -> pd.DataFrame:
    if not phone_id or phone_id == "None" or STATIONARY_INDEX.empty:
        return pd.DataFrame()

    mask = (
        (STATIONARY_INDEX["phone_id"] == phone_id)
        & (STATIONARY_INDEX["session"] == phase)
        & (STATIONARY_INDEX["type"] == "parsed")
    )

    # Filter by sensor in file name
    # In "end" phase, both sensors are in one file starting with "both"
    if phase == "end":
        mask &= STATIONARY_INDEX["file_name"].str.startswith("both")
    else:
        mask &= STATIONARY_INDEX["file_name"].str.startswith(sensor)

    rows = STATIONARY_INDEX[mask]
    frames = []
    for _, row in rows.iterrows():
        path = DATA_DIR / row["path"]
        if path.exists():
            df = pd.read_parquet(path)
            if not df.empty:
                df["file"] = row["file_name"]
                if "file" in df.columns and df["file"].dtype == object:
                    df["file"] = df["file"].map(_get_stem)
                else:
                    df["file"] = Path(row["file_name"]).stem
                df["phase"] = phase.capitalize()
                frames.append(df)
        else:
            print(f"DEBUG: Stationary data file not found: {path.as_posix()}")

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _load_allan_data(phone_id: str | None, sensor: str) -> pd.DataFrame:
    if not phone_id or phone_id == "None" or STATIONARY_INDEX.empty:
        return pd.DataFrame()

    mask = (STATIONARY_INDEX["phone_id"] == phone_id) & (
        STATIONARY_INDEX["type"] == "allan_variance"
    )

    # Start phase: sensor_stationary_...
    # End phase: both_stationary_...
    start_mask = mask & (STATIONARY_INDEX["session"] == "start") & STATIONARY_INDEX["file_name"].str.startswith(sensor)
    end_mask = mask & (STATIONARY_INDEX["session"] == "end") & STATIONARY_INDEX["file_name"].str.startswith("both")
    
    rows = STATIONARY_INDEX[start_mask | end_mask]
    frames = []
    for _, row in rows.iterrows():
        path = DATA_DIR / row["path"]
        if path.exists():
            df = pd.read_parquet(path)
            if not df.empty:
                df["file"] = Path(row["file_name"]).stem
                df["phase"] = row["session"].capitalize()
                frames.append(df)
        else:
            print(f"DEBUG: Data file not found: {path.as_posix()}")

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _load_psd_data(phone_id: str | None, sensor: str) -> pd.DataFrame:
    if not phone_id or phone_id == "None" or STATIONARY_INDEX.empty:
        return pd.DataFrame()

    mask = (STATIONARY_INDEX["phone_id"] == phone_id) & (
        STATIONARY_INDEX["type"] == "power_spectral_density"
    )

    # Start phase: sensor_stationary_...
    # End phase: both_stationary_...
    start_mask = mask & (STATIONARY_INDEX["session"] == "start") & STATIONARY_INDEX["file_name"].str.startswith(sensor)
    end_mask = mask & (STATIONARY_INDEX["session"] == "end") & STATIONARY_INDEX["file_name"].str.startswith("both")
    
    rows = STATIONARY_INDEX[start_mask | end_mask]
    frames = []
    for _, row in rows.iterrows():
        path = DATA_DIR / row["path"]
        if path.exists():
            df = pd.read_parquet(path)
            if not df.empty:
                df["file"] = Path(row["file_name"]).stem
                df["phase"] = row["session"].capitalize()
                frames.append(df)
        else:
            print(f"DEBUG: Data file not found: {path.as_posix()}")

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


def _psd_plot(df: pd.DataFrame, component_prefix: str, title: str):
    if df.empty:
        return _empty_plot("No PSD data")
    if "freq_hz" not in df.columns:
        return _empty_plot("Column not found: freq_hz")

    psd_columns = [
        column
        for column in df.columns
        if column.startswith(component_prefix) and column.endswith("_psd")
    ]
    if not psd_columns:
        return _empty_plot(f"No {title.lower()} columns")

    melted = df.melt(
        id_vars=["freq_hz", "file", "phase"],
        value_vars=psd_columns,
        var_name="component",
        value_name="psd",
    )
    melted["component"] = melted["component"].str.replace("_psd", "", regex=False)

    # Create a unique label for the legend
    melted["legend_label"] = melted["phase"] + " - " + melted["component"]

    fig = px.line(
        melted,
        x="freq_hz",
        y="psd",
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

    @reactive.calc
    def selected_psd_accel_data():
        return _load_psd_data(input.stationary_phone(), "accel")

    @reactive.calc
    def selected_psd_gyro_data():
        return _load_psd_data(input.stationary_phone(), "gyro")

    @output
    @render.data_frame
    def phone_characteristics_table():
        path = PHONE_CHARACTERISTICS_PATH
        if not path.exists():
            print(f"DEBUG: Characteristics file not found: {path.as_posix()}")
            return render.DataTable(pd.DataFrame())
        return render.DataTable(pd.read_parquet(path))

    @output
    @render.data_frame
    def start_characteristics_table():
        path = START_CHARACTERISTICS_PATH
        if not path.exists():
            print(f"DEBUG: Start characteristics file not found: {path.as_posix()}")
            return render.DataTable(pd.DataFrame())
        return render.DataTable(pd.read_parquet(path))

    @output
    @render.data_frame
    def end_characteristics_table():
        path = END_CHARACTERISTICS_PATH
        if not path.exists():
            print(f"DEBUG: End characteristics file not found: {path.as_posix()}")
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

    # PSD Plots
    @output
    @render_plotly
    def stationary_accel_psd_plot():
        return _psd_plot(
            selected_psd_accel_data(),
            "LinAcc",
            "Accel Power Spectral Density",
        )

    @output
    @render_plotly
    def stationary_gyro_psd_plot():
        return _psd_plot(
            selected_psd_gyro_data(),
            "RotVel",
            "Gyro Power Spectral Density",
        )

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
