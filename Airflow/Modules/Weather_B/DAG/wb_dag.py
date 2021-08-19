from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from Weather_ETL_Modules.weather_extraction import wextraction


default_args = {
    "owner" : "airflow", 
    "depends_on_past" : False,
    "start_date" : datetime.now(), 
    "email" : ["airflow@example.com"],
    "email_on_failure" : False,
    "email_on_retry" : False, 
    "retries" : 1,
    "retry_delay" : timedelta(minutes = 1)
}

dag = DAG(
    'Weather_B', 
    default_args=default_args,
    description="Weather_B",
    catchup=False,
    schedule_interval="0 0 * * *"
)


weather_B_run = PythonOperator(
    task_id="weather_project",
    python_callable=wextraction,
    dag=dag
)


weather_B_run 