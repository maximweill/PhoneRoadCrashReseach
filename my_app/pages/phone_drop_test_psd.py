from pathlib import Path
import pandas as pd
import plotly.express as px
from shiny import reactive, render, ui
from shinywidgets import output_widget, render_plotly

from .standard_filter import (
    DROP_PSD_INDEX,
    DATA_DIR,
    filter_drop_psd_index_by_input,
    get_drop_psd_index_filters,
    get_unique_drop_psd,
)

TIME_COLUMN = "freq_hz"
PSD_PLOT_HEIGHT = "100%"


def phone_drop_test_psd_page():
    unique_drops = get_unique_drop_psd()
    if unique_drops.empty:
        return ui.nav_panel("Phone Drop PSD", ui.p("No PSD data available"))

    sidebar_cards = get_drop_psd_index_filters("drop_psd")

    return ui.nav_panel(
        "Phone Drop PSD",
        ui.layout_sidebar(
            ui.sidebar(
                ui.input_action_button(
                    "update_drop_psd_plots",
                    "Update Plots",
                    class_="btn-primary w-100",
                ),
                ui.hr(),
                *sidebar_cards,
                width=350,
            ),
            ui.layout_columns(
                ui.card(
                    ui.card_header("Accelerometer PSD (m²/s⁴/Hz)"),
                    output_widget("drop_psd_accel_plot", height=PSD_PLOT_HEIGHT),
                    full_screen=True,
                ),
                ui.card(
                    ui.card_header("Gyroscope PSD (rad²/s²/Hz)"),
                    output_widget("drop_psd_gyro_plot", height=PSD_PLOT_HEIGHT),
                    full_screen=True,
                ),
            ),
        ),
    )


def _read_psd_sample(path: Path | None, source: str, phone_id: str) -> pd.DataFrame:
    if path is None or not path.exists():
        return pd.DataFrame()

    df = pd.read_parquet(path).copy()
    df["source"] = source
    df["phone_id"] = phone_id
    df["file"] = path.stem
    return df


def _empty_plot(title: str):
    return px.scatter(title=title)


def _plot_psd(df: pd.DataFrame, component_prefix: str, title: str):
    if df.empty:
        return _empty_plot("No data")

    psd_cols = [
        c for c in df.columns
        if c.startswith(component_prefix) and c.endswith("_psd")
    ]

    if not psd_cols:
        return _empty_plot("No PSD columns found")

    melted = df.melt(
        id_vars=["freq_hz", "phone_id", "source", "file"],
        value_vars=psd_cols,
        var_name="component",
        value_name="psd",
    )

    melted["component"] = melted["component"].str.replace("_psd", "", regex=False)
    melted["legend"] = melted["source"] + " - " + melted["component"]

    fig = px.line(
        melted,
        x="freq_hz",
        y="psd",
        color="legend",
        line_dash="source",
        title=title,
    )

    fig.update_xaxes(type="log")
    fig.update_yaxes(type="log")

    fig.update_layout(
        legend=dict(
            orientation="h",
            y=-0.2,
            x=0.5,
            xanchor="center",
            yanchor="top",
        )
    )

    return fig


def register_phone_drop_psd_server(input, output, session):

    @reactive.calc
    @reactive.event(input.update_drop_psd_plots)
    def filtered_psd_index():
        return filter_drop_psd_index_by_input(input, "drop_psd")

    @reactive.calc
    def psd_data():
        rows = filtered_psd_index()
        frames = []

        for row in rows.itertuples(index=False):
            path = DATA_DIR / row.path
            df = _read_psd_sample(path, row.data_type, row.phone_id)
            if not df.empty:
                frames.append(df)

        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    @output
    @render_plotly
    def drop_psd_accel_plot():
        return _plot_psd(psd_data(), "LinAcc", "Accelerometer PSD")

    @output
    @render_plotly
    def drop_psd_gyro_plot():
        return _plot_psd(psd_data(), "RotVel", "Gyroscope PSD")