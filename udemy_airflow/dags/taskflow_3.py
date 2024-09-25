from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime

def _task_e():
    print("Task E")
    return 42

@dag(   
    start_date=datetime(2023, 1,1),
    schedule_interval='@daily',
    catchup=False,
    tags=['taskflow_3']
)

def taskflow_3():

    task_e = PythonOperator(
        task_id='task_e',
        python_callable=_task_e
    )

    @task
    def task_f(value):
        print("Task F")
        print(value)  

    task_f(task_e.output)

taskflow_3()