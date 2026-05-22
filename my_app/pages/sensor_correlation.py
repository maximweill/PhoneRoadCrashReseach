from shiny import ui

from my_app.utils.layout import empty_card


def sensor_correlation_page():
    return ui.nav_panel(
        "Sensor Correlation",
        ui.layout_columns(empty_card("Filters", "220px")),
        ui.layout_columns(
            empty_card("Accelerometer Correlation (m/s2)"),
            empty_card("Gyroscope Correlation (rad/s)"),
            empty_card("Rotational Acceleration Correlation (rad/s2)"),
        ),
        ui.accordion(
            ui.accordion_panel(
                "Linear Acceleration XYZ Components",
                ui.layout_columns(
                    empty_card("LinAcc X"),
                    empty_card("LinAcc Y"),
                    empty_card("LinAcc Z"),
                ),
            ),
            ui.accordion_panel(
                "Rotational Velocity XYZ Components",
                ui.layout_columns(
                    empty_card("RotVel X"),
                    empty_card("RotVel Y"),
                    empty_card("RotVel Z"),
                ),
            ),
            ui.accordion_panel(
                "Rotational Acceleration XYZ Components",
                ui.layout_columns(
                    empty_card("RotAcc X"),
                    empty_card("RotAcc Y"),
                    empty_card("RotAcc Z"),
                ),
            ),
            open=False,
        ),
    )
