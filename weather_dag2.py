import requests
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

API_KEY = "3bd0125aa8cd0eada756ea251e8b2aa6"
CITY = "São Paulo"

# Configurações do PostgreSQL
DB_CONFIG = {
    'dbname': 'climatologia_tech',
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': '5433'
}

@task1
def extrair_dados():
    url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    data = response.json()
    return data

@task2
def transformar_dados():
    data = extrair_dados()
    return {
        "cidade": data["name"],
        "temperatura": data["main"]["temp"],
        "clima": data["weather"][0]["description"]
    }

@task3
def carregar_dados():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Criar tabela se não existir
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS clima (
                id SERIAL PRIMARY KEY,
                cidade VARCHAR(100),
                temperatura DECIMAL(5,2),
                clima VARCHAR(100),
                data_extracao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        data = transformar_dados()
        
        cursor.execute("""
            INSERT INTO clima (cidade, temperatura, clima)
            VALUES (%s, %s, %s)""", 
            (data["cidade"], data["temperatura"], data["clima"])
        )
        
        conn.commit()
        
    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        raise e
    
    finally:
        if conn:
            cursor.close()
            conn.close()

# Definição dos argumentos padrão do DAG
definir_default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

dag = DAG(
    'weather_pipeline',
    default_args=definir_default_args,
    schedule_interval='@daily'
)

task1 = PythonOperator(task_id='extrair', python_callable=extrair_dados, dag=dag)
task2 = PythonOperator(task_id='transformar', python_callable=transformar_dados, dag=dag)
task3 = PythonOperator(task_id='carregar', python_callable=carregar_dados, dag=dag)

task1 >> task2 >> task3

