from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import csv


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 9),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='A DAG to fetch data from API and save to CSV',
    schedule_interval=timedelta(days=1),
)

def fetch_data_from_api(api_url, payload):
    try:
        response = requests.post(api_url, json=payload)
        response.raise_for_status()  # Raise an exception for 4xx/5xx status codes
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

def save_to_csv(data, filename):
    keys = data[0].keys() if data else []
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)

def process_data():
    api_url = "API_ENDPOINT"
    payload = {
        
    }
    api_data = fetch_data_from_api(api_url, payload)

    if api_data:
        # Loop over the fetched data and save it to a list of dictionaries
        processed_data = []
        for item in api_data:
            col1_values = item["col1"].split(",")
            col2_values = item["col2"].split("!~!~!")
            table_data = dict(zip(col1_values, col2_values))
            processed_data.append(table_data)
        
        # Save processed data to CSV
        save_to_csv(processed_data, "/path/to/output.csv")
        print("Data saved to output.csv")
    else:
        print("No data fetched from the API.")

fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=process_data,
    dag=dag,
)

fetch_task
