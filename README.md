# Phone Road Crash Research

A specialized research tool for analyzing road crash data and phone drop tests. This project provides a web-based dashboard to visualize sensor data from car crashes, sled tests, and controlled phone drops, aiding in the study of impact forces and sensor performance.

## Features

- **Phone Drop Test Data**: Interactive visualization of accelerometer and gyroscope data from both phones and headform sensors during drop tests.
- **Crash Data Analysis**: Analysis of standard car crash and sled crash data, including sampling rate distribution and peak force metrics.
- **Sensor Abilities**: A comparative database of phone sensor capabilities (sampling rates, availability) based on the [phyphox sensordb](https://phyphox.org/sensordb).
- **High-Performance Data Processing**: Automated conversion of raw CSV sensor data into Apache Parquet format for efficient storage and fast interactive analysis.

## Project Structure

- `app.py`: The main [Shiny for Python](https://shiny.posit.co/py/) application.
- `parquetify.py`: Data processing script to clean, frame, and convert raw CSVs to Parquet.
- `get_data.py`: Data loading and path management utility.
- `helper.py`: Analytical functions for sensor data processing.
- `car_crash_data_parquet/`: Processed car and sled crash datasets.
- `phone_drop_test_data_parquet/`: Processed phone and headform drop test datasets.
- `phyphox_data/`: Sensor capability database.

## Getting Started

### Prerequisites

- Python 3.9+
- [Recommended] Virtual environment (venv)

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/maximweill/PhoneRoadCrashReseach.git
   cd PhoneRoadCrashReseach
   ```

2. **Set up virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Running the Dashboard

Launch the interactive Shiny app using:

```bash
shiny run --reload app.py
```

Navigate to the local URL provided (typically `http://127.0.0.1:8000`).

### Processing New Data

To process new raw CSV data:

1. Update the log in RAW_LOGS 
2. Place raw CSV files in a new directory `data_processing_gitignore/RAW_DATA`.
3. Rerun phone characteristics for the new dataset
4. Add a pipeline for parsing the data within the parse_raw_data.py, and run it to parse the data
5. Add a pipeline for framing the data (this limits the size used for displaying it), and run it.
6. Add the relavent directory to any analysis scripts and run them
7. update and run parquetify.py to create quick readable files for the webapp.


## Credits

- **Developer**: Maxim Weill
- **Data Source**: Claire Baker (Car and sled crash data), Maxim Weill and Claire Baker (Drop Test)
- **Sensor Database**: [phyphox](https://phyphox.org/sensordb)

---
*Developed for Imperial College London research analysis.*
