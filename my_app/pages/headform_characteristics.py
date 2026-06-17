import re
from pathlib import Path

import pandas as pd
import plotly.express as px
from shiny import reactive, render, ui
from shinywidgets import output_widget, render_plotly


PAGES_DIR = Path(__file__).resolve().parent
APP_DIR = PAGES_DIR.parent
DATA_DIR = APP_DIR / "data"
AGGREGATED_CHARACTERISTICS_PATH = (
    DATA_DIR / "lookup_tables_parquet" / "characteristics" / "phone_characteristics_aggregated.parquet"
)
UNFILTERED_CHARACTERISTICS_PATH = (
    DATA_DIR / "lookup_tables_parquet" / "characteristics" / "headform_characteristics_stationary.parquet"
)
FILTERED_CHARACTERISTICS_PATH = (
    DATA_DIR / "lookup_tables_parquet" / "characteristics" / "headform_filtered_characteristics_stationary.parquet"
)
HEADFORM_CHARACTERISTICS_PATH = (
    DATA_DIR / "lookup_tables_parquet" / "characteristics" / "headform_characteristics.parquet"
)
TIME_COLUMN = "Time (s)"
ACCEL_RES_COLUMN = "LinAccRes (m/s2)"
GYRO_RES_COLUMN = "RotVelRes (rad/s)"
TABLE_CARD_MIN_HEIGHT = "400px"
TRACE_PLOT_HEIGHT = "420px"
ALLAN_PLOT_HEIGHT = "460px"
PSD_PLOT_HEIGHT = "460px"


def _read_headform_index() -> pd.DataFrame:
    stationary_dir = DATA_DIR / "stationary_parquet"
    if not stationary_dir.exists():
        return pd.DataFrame()

    files = []
    # Use rglob to find all parquet files in stationary_parquet
    for p in stationary_dir.rglob("*.parquet"):
        try:
            rel_to_stationary = p.relative_to(stationary_dir)
            parts = rel_to_stationary.parts
            if len(parts) < 2:
                continue

            session = parts[0]  # headform, headform_filtered, start, end
            if "headform" not in session:
                continue
                
            data_type = parts[1]  # parsed, allan_variance, power_spectral_density

            filename = p.name
            match = re.search(r"(Phone\d+|Headform)", filename)
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


HEADFORM_INDEX = _read_headform_index()


def _get_all_headforms() -> list[str]:
    if HEADFORM_INDEX.empty:
        return []
    return sorted(HEADFORM_INDEX["phone_id"].unique().tolist())


HEADFORM_PHONES = _get_all_headforms()


def headform_characteristics_page():
    phone_choices = HEADFORM_PHONES or ["None"]

    return ui.nav_panel(
        "Headform Characteristics",
        ui.card(
            ui.card_header("Headform Impact Characteristics Overview"),
            ui.layout_columns(
                ui.card(
                    ui.card_header("Max Linear Acceleration (m/s2)"),
                    output_widget("headform_impact_accel_box_plot", height=TRACE_PLOT_HEIGHT),
                    full_screen=True,
                ),
                ui.card(
                    ui.card_header("Max Rotational Velocity (rad/s)"),
                    output_widget("headform_impact_rotvel_box_plot", height=TRACE_PLOT_HEIGHT),
                    full_screen=True,
                ),
                ui.card(
                    ui.card_header("Max Rotational Acceleration (rad/s2)"),
                    output_widget("headform_impact_rotacc_box_plot", height=TRACE_PLOT_HEIGHT),
                    full_screen=True,
                ),
                fill=False,
            ),
            fill=False,
        ),
        ui.layout_columns(
            ui.card(
                ui.card_header("Headform Stationary Behavior Analysis"),
                ui.input_select(
                    "headform_phone",
                    "Select Device",
                    choices=phone_choices,
                    selected=phone_choices[0] if phone_choices else "None",
                ),
                fill=False,
            ),
            ui.card(
                ui.card_header("Stationary Noise Floor"),
                ui.output_text("headform_stationary_stats"),
                fill=False,
            ),
            fill=False,
        ),
        ui.layout_columns(
            ui.card(
                ui.card_header("Accelerometer Power Spectral Density"),
                output_widget(
                    "headform_accel_psd_plot",
                    height=PSD_PLOT_HEIGHT,
                ),
                full_screen=True,
                fill=False,
            ),
            ui.card(
                ui.card_header("Gyroscope Power Spectral Density"),
                output_widget(
                    "headform_gyro_psd_plot",
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
                    "headform_accel_allan_plot",
                    height=ALLAN_PLOT_HEIGHT,
                ),
                full_screen=True,
                fill=False,
            ),
            ui.card(
                ui.card_header("Gyroscope Allan Deviation"),
                output_widget(
                    "headform_gyro_allan_plot",
                    height=ALLAN_PLOT_HEIGHT,
                ),
                full_screen=True,
                fill=False,
            ),
            fill=False,
        ),
        ui.accordion(
            ui.accordion_panel(
                "Accelerometer - Unfiltered",
                ui.layout_columns(
                    ui.card(
                        ui.card_header("Accel X (m/s2)"),
                        output_widget("headform_accel_unfiltered_x_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                    ui.card(
                        ui.card_header("Accel Y (m/s2)"),
                        output_widget("headform_accel_unfiltered_y_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                    ui.card(
                        ui.card_header("Accel Z (m/s2)"),
                        output_widget("headform_accel_unfiltered_z_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                ),
                value="accel_unfiltered",
            ),
            ui.accordion_panel(
                "Accelerometer - Filtered (200Hz)",
                ui.layout_columns(
                    ui.card(
                        ui.card_header("Accel X (m/s2)"),
                        output_widget("headform_accel_filtered_x_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                    ui.card(
                        ui.card_header("Accel Y (m/s2)"),
                        output_widget("headform_accel_filtered_y_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                    ui.card(
                        ui.card_header("Accel Z (m/s2)"),
                        output_widget("headform_accel_filtered_z_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                ),
                value="accel_filtered",
            ),
            ui.accordion_panel(
                "Gyroscope - Unfiltered",
                ui.layout_columns(
                    ui.card(
                        ui.card_header("Gyro X (rad/s)"),
                        output_widget("headform_gyro_unfiltered_x_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                    ui.card(
                        ui.card_header("Gyro Y (rad/s)"),
                        output_widget("headform_gyro_unfiltered_y_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                    ui.card(
                        ui.card_header("Gyro Z (rad/s)"),
                        output_widget("headform_gyro_unfiltered_z_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                ),
                value="gyro_unfiltered",
            ),
            ui.accordion_panel(
                "Gyroscope - Filtered (200Hz)",
                ui.layout_columns(
                    ui.card(
                        ui.card_header("Gyro X (rad/s)"),
                        output_widget("headform_gyro_filtered_x_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                    ui.card(
                        ui.card_header("Gyro Y (rad/s)"),
                        output_widget("headform_gyro_filtered_y_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                    ui.card(
                        ui.card_header("Gyro Z (rad/s)"),
                        output_widget("headform_gyro_filtered_z_plot", height=TRACE_PLOT_HEIGHT),
                        full_screen=True,
                    ),
                ),
                value="gyro_filtered",
            ),
            open="accel_unfiltered",
        ),
        ui.card(
            ui.card_header("Aggregated Characteristics"),
            ui.output_data_frame("headform_characteristics_table"),
            full_screen=True,
            min_height=TABLE_CARD_MIN_HEIGHT,
            fill=False,
        ),
        ui.layout_columns(
            ui.card(
                ui.card_header("Unfiltered Characteristics"),
                ui.output_data_frame("headform_unfiltered_characteristics_table"),
                full_screen=True,
                min_height=TABLE_CARD_MIN_HEIGHT,
                fill=False,
            ),
            ui.card(
                ui.card_header("Filtered Characteristics"),
                ui.output_data_frame("headform_filtered_characteristics_table"),
                full_screen=True,
                min_height=TABLE_CARD_MIN_HEIGHT,
                fill=False,
            ),
            fill=False,
        ),
    )


def _load_headform_sensor_data(phone_id: str | None, sensor: str, phase: str) -> pd.DataFrame:
    if not phone_id or phone_id == "None" or HEADFORM_INDEX.empty:
        return pd.DataFrame()

    # phase is 'headform' (unfiltered) or 'headform_filtered'
    mask = (
        (HEADFORM_INDEX["phone_id"] == phone_id)
        & (HEADFORM_INDEX["session"] == phase)
        & (HEADFORM_INDEX["type"] == "parsed")
    )

    rows = HEADFORM_INDEX[mask]
    frames = []
    for _, row in rows.iterrows():
        path = DATA_DIR / row["path"]
        if path.exists():
            df = pd.read_parquet(path)
            if not df.empty:
                df["file"] = Path(row["file_name"]).stem
                df["phase"] = "Filtered" if "filtered" in phase else "Unfiltered"
                frames.append(df)
        else:
            print(f"DEBUG: Headform data file not found: {path.as_posix()}")

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _load_headform_allan_data(phone_id: str | None, sensor: str) -> pd.DataFrame:
    if not phone_id or phone_id == "None" or HEADFORM_INDEX.empty:
        return pd.DataFrame()

    mask = (HEADFORM_INDEX["phone_id"] == phone_id) & (
        HEADFORM_INDEX["type"] == "allan_variance"
    )

    rows = HEADFORM_INDEX[mask]
    frames = []
    for _, row in rows.iterrows():
        path = DATA_DIR / row["path"]
        if path.exists():
            df = pd.read_parquet(path)
            if not df.empty:
                df["file"] = Path(row["file_name"]).stem
                df["phase"] = "Filtered" if "filtered" in row["session"] else "Unfiltered"
                frames.append(df)
        else:
            print(f"DEBUG: Data file not found: {path.as_posix()}")

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _load_headform_psd_data(phone_id: str | None, sensor: str) -> pd.DataFrame:
    if not phone_id or phone_id == "None" or HEADFORM_INDEX.empty:
        return pd.DataFrame()

    mask = (HEADFORM_INDEX["phone_id"] == phone_id) & (
        HEADFORM_INDEX["type"] == "power_spectral_density"
    )

    rows = HEADFORM_INDEX[mask]
    frames = []
    for _, row in rows.iterrows():
        path = DATA_DIR / row["path"]
        if path.exists():
            df = pd.read_parquet(path)
            if not df.empty:
                df["file"] = Path(row["file_name"]).stem
                df["phase"] = "Filtered" if "filtered" in row["session"] else "Unfiltered"
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


def register_headform_characteristics_server(input, output, session):
    @reactive.calc
    def selected_accel_unfiltered_data():
        return _load_headform_sensor_data(input.headform_phone(), "accel", "headform")

    @reactive.calc
    def selected_accel_filtered_data():
        return _load_headform_sensor_data(input.headform_phone(), "accel", "headform_filtered")

    @reactive.calc
    def selected_gyro_unfiltered_data():
        return _load_headform_sensor_data(input.headform_phone(), "gyro", "headform")

    @reactive.calc
    def selected_gyro_filtered_data():
        return _load_headform_sensor_data(input.headform_phone(), "gyro", "headform_filtered")

    @reactive.calc
    def selected_allan_accel_data():
        return _load_headform_allan_data(input.headform_phone(), "accel")

    @reactive.calc
    def selected_allan_gyro_data():
        return _load_headform_allan_data(input.headform_phone(), "gyro")

    @reactive.calc
    def selected_psd_accel_data():
        return _load_headform_psd_data(input.headform_phone(), "accel")

    @reactive.calc
    def selected_psd_gyro_data():
        return _load_headform_psd_data(input.headform_phone(), "gyro")

    @output
    @render.data_frame
    def headform_characteristics_table():
        path = AGGREGATED_CHARACTERISTICS_PATH
        if not path.exists():
            return render.DataTable(pd.DataFrame())
        df = pd.read_parquet(path)
        # Filter only Headform
        return render.DataTable(df[df["phone_id"] == "Headform"])

    @output
    @render.data_frame
    def headform_unfiltered_characteristics_table():
        path = UNFILTERED_CHARACTERISTICS_PATH
        if not path.exists():
            return render.DataTable(pd.DataFrame())
        return render.DataTable(pd.read_parquet(path))

    @output
    @render.data_frame
    def headform_filtered_characteristics_table():
        path = FILTERED_CHARACTERISTICS_PATH
        if not path.exists():
            return render.DataTable(pd.DataFrame())
        return render.DataTable(pd.read_parquet(path))

    @output
    @render.text
    def headform_stationary_stats():
        acc_u = selected_accel_unfiltered_data()
        acc_f = selected_accel_filtered_data()
        gyr_u = selected_gyro_unfiltered_data()
        gyr_f = selected_gyro_filtered_data()

        stats = []
        if not acc_u.empty and ACCEL_RES_COLUMN in acc_u.columns:
            stats.append(f"Accel Unfiltered RMS: {acc_u[ACCEL_RES_COLUMN].std():.4f} m/s2")
        if not acc_f.empty and ACCEL_RES_COLUMN in acc_f.columns:
            stats.append(f"Accel Filtered RMS: {acc_f[ACCEL_RES_COLUMN].std():.4f} m/s2")
        if not gyr_u.empty and GYRO_RES_COLUMN in gyr_u.columns:
            stats.append(f"Gyro Unfiltered RMS: {gyr_u[GYRO_RES_COLUMN].std():.4f} rad/s")
        if not gyr_f.empty and GYRO_RES_COLUMN in gyr_f.columns:
            stats.append(f"Gyro Filtered RMS: {gyr_f[GYRO_RES_COLUMN].std():.4f} rad/s")

        return " | ".join(stats) if stats else "No data selected."

    # PSD Plots
    @output
    @render_plotly
    def headform_accel_psd_plot():
        return _psd_plot(
            selected_psd_accel_data(),
            "LinAcc",
            "Accel Power Spectral Density",
        )

    @output
    @render_plotly
    def headform_gyro_psd_plot():
        return _psd_plot(
            selected_psd_gyro_data(),
            "RotVel",
            "Gyro Power Spectral Density",
        )

    # Allan Plots
    @output
    @render_plotly
    def headform_accel_allan_plot():
        return _allan_plot(
            selected_allan_accel_data(),
            "LinAcc",
            "Accel Allan Deviation",
        )

    @output
    @render_plotly
    def headform_gyro_allan_plot():
        return _allan_plot(
            selected_allan_gyro_data(),
            "RotVel",
            "Gyro Allan Deviation",
        )

    # Accel Unfiltered Plots
    @output
    @render_plotly
    def headform_accel_unfiltered_x_plot():
        return _stationary_line_plot(
            selected_accel_unfiltered_data(), "LinAccX (m/s2)", "Accel X - Unfiltered"
        )

    @output
    @render_plotly
    def headform_accel_unfiltered_y_plot():
        return _stationary_line_plot(
            selected_accel_unfiltered_data(), "LinAccY (m/s2)", "Accel Y - Unfiltered"
        )

    @output
    @render_plotly
    def headform_accel_unfiltered_z_plot():
        return _stationary_line_plot(
            selected_accel_unfiltered_data(), "LinAccZ (m/s2)", "Accel Z - Unfiltered"
        )

    # Accel Filtered Plots
    @output
    @render_plotly
    def headform_accel_filtered_x_plot():
        return _stationary_line_plot(
            selected_accel_filtered_data(), "LinAccX (m/s2)", "Accel X - Filtered"
        )

    @output
    @render_plotly
    def headform_accel_filtered_y_plot():
        return _stationary_line_plot(
            selected_accel_filtered_data(), "LinAccY (m/s2)", "Accel Y - Filtered"
        )

    @output
    @render_plotly
    def headform_accel_filtered_z_plot():
        return _stationary_line_plot(
            selected_accel_filtered_data(), "LinAccZ (m/s2)", "Accel Z - Filtered"
        )

    # Gyro Unfiltered Plots
    @output
    @render_plotly
    def headform_gyro_unfiltered_x_plot():
        return _stationary_line_plot(
            selected_gyro_unfiltered_data(), "RotVelX (rad/s)", "Gyro X - Unfiltered"
        )

    @output
    @render_plotly
    def headform_gyro_unfiltered_y_plot():
        return _stationary_line_plot(
            selected_gyro_unfiltered_data(), "RotVelY (rad/s)", "Gyro Y - Unfiltered"
        )

    @output
    @render_plotly
    def headform_gyro_unfiltered_z_plot():
        return _stationary_line_plot(
            selected_gyro_unfiltered_data(), "RotVelZ (rad/s)", "Gyro Z - Unfiltered"
        )

    # Gyro Filtered Plots
    @output
    @render_plotly
    def headform_gyro_filtered_x_plot():
        return _stationary_line_plot(
            selected_gyro_filtered_data(), "RotVelX (rad/s)", "Gyro X - Filtered"
        )

    @output
    @render_plotly
    def headform_gyro_filtered_y_plot():
        return _stationary_line_plot(
            selected_gyro_filtered_data(), "RotVelY (rad/s)", "Gyro Y - Filtered"
        )

    @output
    @render_plotly
    def headform_gyro_filtered_z_plot():
        return _stationary_line_plot(
            selected_gyro_filtered_data(), "RotVelZ (rad/s)", "Gyro Z - Filtered"
        )

    def _impact_box_plot(column: str, title: str, color: str):
        path = HEADFORM_CHARACTERISTICS_PATH
        print(f"DEBUG: Loading box plot data from {path}")
        if not path.exists():
            print(f"DEBUG: Path does not exist: {path}")
            return _empty_plot("No impact characteristics data")
        
        try:
            df = pd.read_parquet(path)
            print(f"DEBUG: Successfully read parquet. Shape: {df.shape}")
            if column not in df.columns:
                print(f"DEBUG: Column {column} not found in {df.columns.tolist()}")
                return _empty_plot(f"Column not found: {column}")
                
            fig = px.box(
                df,
                y=column,
                points="all",
                title=title,
            )
            fig.update_traces(marker_color=color)
            return fig
        except Exception as e:
            print(f"DEBUG: Error in _impact_box_plot: {e}")
            return _empty_plot(f"Error: {str(e)}")

    @output
    @render_plotly
    def headform_impact_accel_box_plot():
        return _impact_box_plot(
            "max_LinAccRes (m/s2)", 
            "Max Linear Acceleration",
            "#636EFA"
        )

    @output
    @render_plotly
    def headform_impact_rotvel_box_plot():
        return _impact_box_plot(
            "max_RotVelRes (rad/s)", 
            "Max Rotational Velocity",
            "#EF553B"
        )

    @output
    @render_plotly
    def headform_impact_rotacc_box_plot():
        return _impact_box_plot(
            "max_RotAccRes (rad/s2)", 
            "Max Rotational Acceleration",
            "#00CC96"
        )
