from shiny import ui

from my_app.utils.layout import empty_card


def home_page():
    return ui.nav_panel(
        "Home",
        ui.h2("Phone Road Crash Research Portal"),
        ui.p(
            "Skeleton Shiny Core app for investigating smartphone sensor "
            "performance in road crash scenarios."
        ),
        ui.layout_columns(
            empty_card("Phone Drop Test Data", "180px"),
            empty_card("Crash Data", "180px"),
            empty_card("Sensor Abilities", "180px"),
        ),
        empty_card("Project Notes", "180px"),
    )
