from airflow.decorators import dag, task
from datetime import datetime, timedelta
import random

@dag(    
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    description='A simple DAG to generate and check random numbers',
    catchup=False
)

def project_2():

    @task
    def generate_random_number(**context):
        ti = context['ti']
        number = random.randint(1, 100)
        ti.xcom_push(key='random_number', value=number)
        print(f"Generated random number: {number}")

    @task
    def check_even_odd(number):               
        result = "even" if number % 2 == 0 else "odd"
        print(f"The number {number} is {result}.")
        print(number)

    check_even_odd(generate_random_number())

project_2()