from shiny import App, ui

from my_app.pages import *
from my_app.utils import EMPTY_CARD_CSS


app_ui = ui.page_navbar(
    home_page(),
    tested_phone_characteristics_page(),
    headform_characteristics_page(),

    phone_drop_test_page(),
    phone_drop_test_psd_page(),

    calibration_tests_page(),

    sensor_correlation_page(),
    sensor_agreement_page(),

    sensor_abilities_page(),

    title="Phone Road Crash Research",
    fillable=True,
    header=ui.tags.style(EMPTY_CARD_CSS),
)

def server(input, output, session):
    register_tested_phone_characteristics_server(input, output, session)
    register_headform_characteristics_server(input, output, session)

    register_phone_drop_test_server(input, output, session)
    register_calibration_tests_server(input, output, session)

    register_sensor_correlation_server(input, output, session)
    register_sensor_agreement_server(input, output, session)

    register_sensor_abilities_server(input, output, session)
    register_phone_drop_psd_server(input, output, session)


app = App(app_ui, server)
