from airflow import DAG
from airflow.decorators import task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from datetime import datetime, timedelta
import requests
import pandas as pd

with DAG(
    dag_id="weather_etl",
    start_date=datetime(2024, 1, 1, 9),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    }
) as dag:
    @task
    def hit_weather_api(**context):
        city = context.get("city", "Fortaleza")  # Make city configurable via context
        api_key = context.get("api_key", "<3bd0125aa8cd0eada756ea251e8b2aa6>")  # Retrieve API key from context
        
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
        response = requests.get(url)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise ValueError(f"API request to {city} failed with status code: {response.status_code}")
    
    @task
    def flatten_weather_data(polygon_response, **context):
        columns = {
            "status": None,
            "from_datetime": context.get("ds"),
            "latitude": polygon_response.get("lat", None),
            "longitude": polygon_response.get("lon", None),
            "temperature": polygon_response.get("main.temp", 0),
            "humidity": polygon_response.get("main.humidity", 0),
            "wind_speed": polygon_response.get("wind.speed", 0)
        }
        
        return pd.DataFrame([columns], columns=list(columns.keys()))
    
    @task
    def load_weather_data(flattened_dataframe, connection_name="weather_database_conn"):
        # Use SqliteHook to get the engine
        sqlite_hook = SqliteHook(weather_database_conn)
        engine = sqlite_hook.get_sqlalchemy_engine()
        
        if not flattened_dataframe.empty:
            flattened_dataframe.to_sql(
                name="weather_data",
                con=engine,
                if_exists="append",
                index=False
            )
    
    # Define the dependencies correctly
    raw_weather_response = hit_weather_api(city="Fortaleza")
    transformed_df = flatten_weather_data(raw_weather_response)
    load_weather_data(flattened_dataframe=transformed_df)
