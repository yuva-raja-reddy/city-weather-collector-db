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
import os
import sys
import psycopg2
from psycopg2 import sql
# Suppress all warnings
warnings.filterwarnings("ignore")


# Configure logging
logging.basicConfig(
    filename='weather_data.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def check_postgresql_status():
    """
    Checks if the PostgreSQL service is running.
    
    Returns:
        bool: True if PostgreSQL is running, False otherwise.
    """
    system = platform.system()
    
    try:
        if system == "Darwin":  # macOS
            result = subprocess.run(["brew", "services", "list"], capture_output=True, text=True)
            return "postgresql" in result.stdout and "started" in result.stdout

        elif system == "Linux":
            result = subprocess.run(["systemctl", "is-active", "--quiet", "postgresql"], capture_output=True)
            return result.returncode == 0

        elif system == "Windows":
            result = subprocess.run(["sc", "query", "postgresql-x64-14"], capture_output=True, text=True)
            return "RUNNING" in result.stdout

        else:
            logging.error(f"Unsupported operating system: {system}")
            raise EnvironmentError(f"Unsupported operating system: {system}")

    except subprocess.CalledProcessError as e:
        logging.error(f"Error checking PostgreSQL service status: {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred while checking service status: {e}")
        return False

def start_postgresql():
    """
    Starts the PostgreSQL service in a system-agnostic way if it is not already running.
    Works for Windows, macOS (with Homebrew), and Linux (with systemd or service).
    """
    if check_postgresql_status():
        logging.info("PostgreSQL service is already running.")
        return
    
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
            # Windows needs to be run with administrative privileges
            try:
                subprocess.run(["sc", "start", "postgresql-x64-14"], check=True)
                logging.info("PostgreSQL service started successfully on Windows.")
            except subprocess.CalledProcessError as e:
                logging.error(f"Error starting PostgreSQL service on Windows: {e}")
                # Provide instructions to the user
                logging.error("Ensure you have administrative privileges to start the PostgreSQL service.")
        
        else:
            logging.error(f"Unsupported operating system: {system}")
            raise EnvironmentError(f"Unsupported operating system: {system}")

    except subprocess.CalledProcessError as e:
        logging.error(f"Error starting PostgreSQL service: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

def create_database_if_not_exists(db_url: str):
    """
    Creates the database if it does not exist.
    
    Args:
        db_url (str): The full database URL including the database name.
    """
    # Remove the database name from the URL to connect to the default database
    base_url = db_url.rsplit('/', 1)[0]  # Excludes the database name
    database_name = db_url.rsplit('/', 1)[1]  # Extracts the database name

    # Construct the connection URL for the default 'postgres' database
    conn_url = base_url + '/postgres'

    try:
        # Connect to PostgreSQL using psycopg2
        conn = psycopg2.connect(conn_url)
        conn.autocommit = True  # Ensure auto-commit mode to run CREATE DATABASE
        with conn.cursor() as cursor:
            try:
                # Attempt to create the database
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
                logging.info(f"Database '{database_name}' created successfully.")
            except psycopg2.errors.DuplicateDatabase:
                logging.info(f"Database '{database_name}' already exists.")
            except psycopg2.Error as e:
                logging.error(f"Error creating database '{database_name}': {e}")
    except Exception as e:
        logging.error(f"Error connecting to PostgreSQL server: {e}")
    finally:
        if conn:
            conn.close()


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
    # Load the configuration from config.json
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)

    # Extract database URL from the config
    db_url = config['db_url']

    # Create the database if it does not exist
    create_database_if_not_exists(db_url)

    # Start PostgreSQL service before running the rest of the script
    start_postgresql()

    # Instantiate the WeatherDataCollector with config data
    collector = WeatherDataCollector(config)

    # Instantiate and start the scheduler
    scheduler = WeatherScheduler(collector, interval_seconds=10)
    scheduler.schedule_job()
    scheduler.start()

