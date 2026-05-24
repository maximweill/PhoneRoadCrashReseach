from shiny import App, ui

from my_app.pages import (
    home_page,
    phone_drop_test_page,
    register_phone_drop_test_server,
    register_sensor_abilities_server,
    register_tested_phone_characteristics_server,
    sensor_abilities_page,
    sensor_correlation_page,
    tested_phone_characteristics_page,
)
from my_app.utils import EMPTY_CARD_CSS


app_ui = ui.page_navbar(
    home_page(),
    tested_phone_characteristics_page(),
    phone_drop_test_page(),
    sensor_correlation_page(),
    sensor_abilities_page(),
    title="Phone Road Crash Research",
    fillable=True,
    header=ui.tags.style(EMPTY_CARD_CSS),
)


def server(input, output, session):
    register_tested_phone_characteristics_server(input, output, session)
    register_phone_drop_test_server(input, output, session)
    register_sensor_abilities_server(input, output, session)


app = App(app_ui, server)
