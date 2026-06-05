from my_app.pages.home import home_page
from my_app.pages.phone_drop_test import (
    phone_drop_test_page,
    register_phone_drop_test_server,
)
from my_app.pages.calibration_tests import (
    calibration_tests_page,
    register_calibration_tests_server,
)
from my_app.pages.sensor_abilities import (
    register_sensor_abilities_server,
    sensor_abilities_page,
)
from my_app.pages.sensor_correlation import (
    register_sensor_correlation_server,
    sensor_correlation_page,
)
from my_app.pages.sensor_agreement import (
    register_sensor_agreement_server,
    sensor_agreement_page,
)
from my_app.pages.tested_phone_characteristics import (
    register_tested_phone_characteristics_server,
    tested_phone_characteristics_page,
)
from my_app.pages.headform_characteristics import (
    register_headform_characteristics_server,
    headform_characteristics_page,
)
from my_app.pages.phone_drop_test_psd import (
    phone_drop_test_psd_page,
    register_phone_drop_psd_server,
)


__all__ = [
    "home_page",
    "phone_drop_test_page",
    "register_phone_drop_test_server",
    "phone_drop_test_psd_page",
    "register_phone_drop_psd_server",
    "calibration_tests_page",
    "register_calibration_tests_server",
    "register_sensor_abilities_server",
    "sensor_abilities_page",
    "register_sensor_correlation_server",
    "sensor_correlation_page",
    "register_sensor_agreement_server",
    "sensor_agreement_page",
    "register_tested_phone_characteristics_server",
    "tested_phone_characteristics_page",
    "register_headform_characteristics_server",
    "headform_characteristics_page",
]