# City Weather Data Collector

This project fetches weather data for a specified city using the OpenWeatherMap API, processes the data, and stores it in a PostgreSQL database. The PostgreSQL service is automatically started from the Python code, so there's no need to start it manually.

## Project Overview

The project collects real-time weather data, including temperature, humidity, wind speed, and weather conditions, from the OpenWeatherMap API for the specified city.
The data is cleaned and processed, such as converting temperature from Kelvin to Celsius and removing any unnecessary fields.
After cleaning, the data is stored in a PostgreSQL database, allowing for long-term storage and analysis of historical weather data.
The data collection process is automated and scheduled to run at regular intervals, making the data collection continuous and seamless.

---

<p align="center">
  <img src="https://github.com/yuva-raja-reddy/city-weather-collector-db/blob/main/images/pipeline.png" alt="Weather Data Collection Pipeline" width="500">
</p>

<p align="center"><em>"Weather Data Collection Pipeline"</em></p>

---

## Features
- Fetches weather data (temperature, humidity, wind speed, etc.)
- Cleans and processes the data (e.g., converts temperature to Celsius)
- Stores the data in a PostgreSQL database
- Scheduled weather data collection at regular intervals
- PostgreSQL database is auto-started from the code

## Requirements
- Python 3.x
- PostgreSQL
- OpenWeatherMap API key (You can get your API key by signing up at [OpenWeatherMap](https://home.openweathermap.org/users/sign_up))

## Setup

### 1. Clone the repository:
```bash
git clone https://github.com/yuva-raja-reddy/city-weather-collector-db.git
cd city-weather-collector-db
```

### 2. Create and activate a virtual environment:
```bash
python3 -m venv weather_project
source weather_project/bin/activate
```

### 3. Install dependencies:
```bash
pip install -r requirements.txt
```

### 4. Configure the environment:
- Rename `config.example.json` to `config.json`.
- Add your OpenWeatherMap API key and database connection details in `config.json` or use environment variables.

### 5. Run the weather data collection script:
The PostgreSQL service will automatically start when you run the script.
```bash
python extract_city_weather.py
```

### 6. Verify Data Collection in PostgreSQL:
After running the script, you can check the collected weather data by connecting to your PostgreSQL database.

1. Open the terminal:
   ```bash
   psql -U YOUR_USERNAME -d weather_data
   ```

2. View the weather data in the `weather` table:
   ```sql
   SELECT * FROM weather;
   ```

3. Example output:
   ```
    city    | temperature (°C) | humidity (%) | wind_speed (m/s) | weather |        timestamp        
   ---------+-------------------+--------------+------------------+---------+-------------------------
    Buffalo |              22.56 |           50 |              2.5 | Clear   | 2024-09-11 14:20:01.123
   ```

## Environment Variables (Optional)
You can configure the following environment variables for security:
- `OPENWEATHER_API_KEY`: Your OpenWeatherMap API key.
- `DATABASE_URL`: PostgreSQL connection URL.

```bash
export OPENWEATHER_API_KEY=your_api_key
export DATABASE_URL=your_database_url
```

