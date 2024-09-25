from airflow.decorators import dag, task
from datetime import datetime

@dag(   
    start_date=datetime(2023, 1,1),
    schedule_interval='@daily',
    catchup=False,
    tags=['taskflow_2']
)

def taskflow_2():

    @task
    def task_c():
        print("Task C")
        return 42

    @task
    def task_d(value):
        print("Task D")
        print(value)  

    task_d(task_c())

taskflow_2()