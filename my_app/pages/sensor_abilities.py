import plotly.express as px
from shiny import reactive, ui
from shinywidgets import output_widget, render_plotly

from my_app.utils.sensor_abilities import (
    availability_column,
    devices_data,
    manufacturers,
    numeric_columns,
)


DEFAULT_MANUFACTURER = "Apple"
DEFAULT_VARIABLE = "accelerometer_rate"
PLOT_HEIGHT = "100%"
HISTOGRAM_HEIGHT = "100%"


def _default_choice(choices: list[str], preferred: str = None) -> str | None:
    if preferred and preferred in choices:
        return preferred
    return choices[-1] if choices else None


def sensor_abilities_page():
    manufacturer_choices = manufacturers()
    variable_choices = numeric_columns()

    return ui.nav_panel(
        "Sensor Abilities",
        ui.layout_columns(
            ui.card(
                ui.card_header("Filters"),
                ui.input_select(
                    "sensor_manufacturer",
                    "Manufacturer",
                    choices=manufacturer_choices,
                    selected=_default_choice(manufacturer_choices),
                ),
                ui.input_text(
                    "sensor_model_text",
                    "Text contained in Model Name",
                    value="",
                ),
                ui.input_select(
                    "sensor_variable",
                    "Variable",
                    choices=variable_choices,
                    selected=_default_choice(variable_choices),
                ),
                fill=False,
            ),
            ui.card(
                ui.card_header("Distribution"),
                output_widget("sensor_distribution_plot", height=PLOT_HEIGHT),
                full_screen=True,
                fill=True,
            ),
            ui.card(
                ui.card_header("Availability"),
                output_widget("sensor_availability_plot", height=PLOT_HEIGHT),
                full_screen=True,
                fill=True,
            ),
            fill=False,
        ),
        ui.accordion(
            ui.accordion_panel(
                "Ordered Histogram by Model",
                ui.card(
                    ui.card_header("Histogram"),
                    output_widget("sensor_histogram_plot", height=HISTOGRAM_HEIGHT),
                    full_screen=True,
                    fill=True,
                ),
                value="sensor_histogram",
            ),
            open="sensor_histogram",
        ),
    )


def register_sensor_abilities_server(input, output, session):
    @reactive.calc
    def filtered_data():
        sub = devices_data()
        manufacturer = input.sensor_manufacturer()
        model_text = input.sensor_model_text().strip()

        if manufacturer:
            sub = sub[sub["manufacturer"] == manufacturer]
        if model_text:
            sub = sub[sub["model"].str.contains(model_text, case=False, na=False)]

        return sub

    @output
    @render_plotly
    def sensor_distribution_plot():
        sub = filtered_data()
        variable = input.sensor_variable()

        if sub.empty:
            ui.notification_show("No data available.", duration=2)
            return px.scatter(title="No data available")
        if variable not in sub.columns:
            ui.notification_show(f"Column '{variable}' not found in data.", duration=2)
            return px.scatter(title=f"Column '{variable}' not found")
        if sub[variable].dropna().empty:
            ui.notification_show(f"No valid data for '{variable}'.", duration=2)
            return px.scatter(title=f"No valid data for {variable}")

        return px.box(
            sub,
            y=variable,
        ).update_layout(
            title=f"Distribution of {variable.replace('_', ' ').title()}",
        )

    @output
    @render_plotly
    def sensor_availability_plot():
        sub = filtered_data()
        variable = input.sensor_variable()
        column = availability_column(variable)

        if sub.empty:
            ui.notification_show("No data available.", duration=2)
            return px.scatter(title="No data available")
        if column not in sub.columns:
            ui.notification_show(f"Column '{column}' not found in data.", duration=2)
            return px.scatter(title=f"Column '{column}' not found")

        counts = sub[column].value_counts().reset_index()
        counts.columns = [column, "count"]
        title_text = " ".join(variable.split("_")[:-1]).title()

        return px.pie(
            counts,
            names=column,
            values="count",
            color=column,
            color_discrete_map={True: "blue", False: "red"},
        ).update_layout(
            title=f"Availability of {title_text}",
        )

    @output
    @render_plotly
    def sensor_histogram_plot():
        sub = filtered_data()
        variable = input.sensor_variable()

        if sub.empty:
            ui.notification_show("No data available.", duration=2)
            return px.scatter(title="No data available")
        if variable not in sub.columns:
            ui.notification_show(f"Column '{variable}' not found in data.", duration=2)
            return px.scatter(title=f"Column '{variable}' not found")

        ordered = sub.sort_values(variable)

        return px.bar(
            ordered,
            x="model",
            y=variable,
        ).update_layout(
            title="Ordered Histogram by Model",
            xaxis_title="Model",
            yaxis_title=variable,
        )
