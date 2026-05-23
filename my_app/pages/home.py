from shiny import ui


def home_page():
    return ui.nav_panel(
        "Home",
        ui.div(
            ui.markdown(
                """
                # Phone Road Crash Research Portal
                Welcome to the research dashboard for investigating smartphone sensor performance in road crash scenarios.

                This specialized research tool analyzes road crash data and phone drop tests to visualize sensor data from car crashes, sled tests, and controlled phone drops, aiding in the study of impact forces and sensor performance.

                ### Data Modules
                * **Phone Drop Test Data**: Controlled laboratory tests conducted at Imperial College London (Dyson School of Design Engineering).
                * **Crash Data**: Real-world and sled-based car crash records.
                * **Sensor Abilities**: Global smartphone hardware specifications from the [Phyphox Sensor Database](https://phyphox.org/sensordb).

                ### Project Contributors
                * **Maxim Weill**: Developer and Lead Researcher for drop test data collection.
                * **[Dr. Claire Baker](https://www.imperial.ac.uk/people/claire.baker)**: Schmidt AI for Science Research Fellow at Imperial College London. Dr. Baker's work focuses on trauma biomechanics and using real-time sensor data to predict injuries in road traffic collisions.

                ---
                _Developed for research analysis at Imperial College London._
                """
            ),
            style="max-width: 900px;"
        )
    )
