import warnings
import subprocess
import platform
import requests
import pandas as pd
from sqlalchemy import create_engine
import schedule
import time
from datetime import datetime
import logging
import json

# Suppress all warnings
warnings.filterwarnings("ignore")

# Configure logging
logging.basicConfig(
    filename='weather_data.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def start_postgresql():
    """
    Starts the PostgreSQL service in a system-agnostic way.
    Works for Windows, macOS (with Homebrew), and Linux (with systemd or service).
    """
    system = platform.system()

    try:
        if system == "Darwin":  # macOS
            subprocess.run(["brew", "services", "start", "postgresql"], check=True)
            logging.info("PostgreSQL service started successfully on macOS.")

        elif system == "Linux":
            try:
                subprocess.run(["systemctl", "start", "postgresql"], check=True)
                logging.info("PostgreSQL service started successfully on Linux (systemd).")
            except subprocess.CalledProcessError:
                subprocess.run(["service", "postgresql", "start"], check=True)
                logging.info("PostgreSQL service started successfully on Linux (service).")

        elif system == "Windows":
            try:
                subprocess.run(["net", "start", "postgresql"], check=True)
                logging.info("PostgreSQL service started successfully on Windows (net).")
            except subprocess.CalledProcessError:
                subprocess.run(["sc", "start", "postgresql-x64-14"], check=True)
                logging.info("PostgreSQL service started successfully on Windows (sc).")
        
        else:
            logging.error(f"Unsupported operating system: {system}")
            raise EnvironmentError(f"Unsupported operating system: {system}")

    except subprocess.CalledProcessError as e:
        logging.error(f"Error starting PostgreSQL service: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


class WeatherDataCollector:
    """
    A class responsible for collecting, cleaning, and storing weather data.
    """

    def __init__(self, config: dict):
        """
        Initializes the WeatherDataCollector with necessary configurations.

        Args:
            config (dict): Configuration dictionary with API key, city, and database URL.
        """
        self.city = config['city']
        self.api_key = config['api_key']
        self.db_url = config['db_url']
        logging.info(f"Initialized WeatherDataCollector for city: {self.city}")

    def get_weather_data(self) -> dict:
        """
        Fetches the weather data from the OpenWeatherMap API.

        Returns:
            dict: A dictionary containing the weather information with column names initialized with units.
        """
        url = f"http://api.openweathermap.org/data/2.5/weather?q={self.city}&appid={self.api_key}"
        
        try:
            logging.info(f"Fetching weather data for city: {self.city}")
            response = requests.get(url)
            response.raise_for_status()  # Raise error for bad responses (4xx, 5xx)
            data = response.json()

            # Extract relevant data with column names including units
            weather_info = {
                'city': self.city,
                'temperature (°C)': data['main']['temp'],
                'humidity (%)': data['main']['humidity'],
                'wind_speed (m/s)': data['wind']['speed'],
                'weather': data['weather'][0]['description'],
                'timestamp': datetime.now()
            }
            logging.info(f"Weather data fetched successfully for city: {self.city}")
            return weather_info

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching weather data for city {self.city}: {e}")
            return {}

    def clean_weather_data(self, data: dict) -> pd.DataFrame:
        """
        Cleans the weather data by converting temperature from Kelvin to Celsius, rounding 
        temperature to 2 decimal places.

        Args:
            data (dict): The raw weather data fetched from the API.

        Returns:
            pd.DataFrame: A cleaned DataFrame with temperature rounded to 2 decimal places.
        """
        if not data:
            logging.warning("No weather data to clean.")
            return pd.DataFrame()

        df = pd.DataFrame([data])

        # Convert temperature from Kelvin to Celsius and round to 2 decimal places
        df['temperature (°C)'] = (df['temperature (°C)'] - 273.15).round(2)

        logging.info(f"Weather data cleaned for city: {self.city}")
        return df

    def store_weather_data(self, df: pd.DataFrame):
        """
        Stores the cleaned weather data into the PostgreSQL database.

        Args:
            df (pd.DataFrame): The cleaned weather data in a DataFrame.
        """
        if df.empty:
            logging.warning("No data to store.")
            return

        try:
            engine = create_engine(self.db_url)
            df.to_sql('weather', con=engine, if_exists='append', index=False)
            logging.info(f"Data successfully stored in the database for city: {self.city}")

        except Exception as e:
            logging.error(f"Error while storing data for city {self.city}: {e}")

    def run(self):
        """
        Executes the process of fetching, cleaning, and storing weather data.
        """
        weather_data = self.get_weather_data()
        clean_data = self.clean_weather_data(weather_data)
        self.store_weather_data(clean_data)


class WeatherScheduler:
    """
    A class that manages the scheduling of weather data collection.
    """

    def __init__(self, collector: WeatherDataCollector, interval_seconds: int = 10):
        """
        Initializes the WeatherScheduler to run the weather data collection job at intervals.

        Args:
            collector (WeatherDataCollector): The WeatherDataCollector instance.
            interval_seconds (int): The interval in seconds for the job to run.
        """
        self.collector = collector
        self.interval_seconds = interval_seconds
        logging.info(f"WeatherScheduler initialized to run every {self.interval_seconds} seconds.")

    def schedule_job(self):
        """
        Schedules the weather data collection job.
        """
        schedule.every(self.interval_seconds).seconds.do(self.collector.run)
        logging.info(f"Scheduled job to run every {self.interval_seconds} seconds.")

    def start(self):
        """
        Starts the scheduler to run the job at defined intervals.
        """
        logging.info("Starting the scheduler...")
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":
    # Start PostgreSQL service before running the rest of the script
    start_postgresql()

    # Load the configuration from config.json
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)

    # Instantiate the WeatherDataCollector with config data
    collector = WeatherDataCollector(config)

    # Instantiate and start the scheduler
    scheduler = WeatherScheduler(collector, interval_seconds=10)
    scheduler.schedule_job()
    scheduler.start()
