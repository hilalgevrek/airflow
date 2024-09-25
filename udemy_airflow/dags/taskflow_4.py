from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime

def _task_g():
    print("Task G")
    return 42

@dag(   
    start_date=datetime(2023, 1,1),
    schedule_interval='@daily',
    catchup=False,
    tags=['taskflow_4']
)

def taskflow_4():

    task_g = PythonOperator(
        task_id='task_g',
        python_callable=_task_g
    )

    @task
    def task_h():
        print("Task H")    

    task_g >> task_h()

taskflow_4()