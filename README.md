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

1. Place raw CSV files in the respective `*_ignore` directories (e.g., `car_crash_data_ignore/`).
2. Configure the source and destination paths in the `if __name__ == "__main__":` block of `parquetify.py`.
3. Run the script:
   ```bash
   python parquetify.py
   ```

The script will automatically detect the data type, apply coordinate system transformations, frame the data around the impact event, and save it as a compressed Parquet file.

## Credits

- **Developer**: Maxim Weill
- **Data Source**: Claire Baker (Car and sled crash data), Maxim Weill and Claire Baker (Drop Test)
- **Sensor Database**: [phyphox](https://phyphox.org/sensordb)

---
*Developed for Imperial College London research analysis.*
