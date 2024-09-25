from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue 
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from datetime import datetime
import requests

from include.stock_market.tasks import _get_stock_prices, _store_prices, _get_formatted_csv, BUCKET_NAME

SYMBOL = 'AAPL'

@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup= False,
    tags=['stock_market']
)

def stock_market():
    
    # 1. Checking API availability with the Sensor decorator
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers=api.extra_dejson["headers"])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    # Kendi sensörünü yapmak zorunda değilsin aşağıdaki adresten sağlanan bir çok sensör var. 
    # https://registry.astronomer.io/

    # Çalışıp çalışmadığını test etmek için aşağıdakileri yap.
    # astro dev bash // access the Docker containers running airflow, and then you have access to the airflow CLI.
    # airflow tasks test stock_market is_api_available 2023-01-01
    
    
    # 2. Fetching stock prices with the PythonOperator
    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,    
        op_kwargs={'url':'{{task_instance.xcom_pull(task_ids="is_api_available")}}', 'symbol': SYMBOL}    
    )

    # airflow tasks test stock_market get_stock_market_prices 2023-01-01
        

    # 3. Storing stock prices in MinIO (AWS S3 like) 
    # This task starts the prices that you fetch in the previous task into Minio as a JSON file.
    # Minio is like aws, s3 or google cloud storage.
    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={'stock': '{{task_instance.xcom_pull(task_ids="get_stock_prices")}}'}
    )

    # spark is a data processing framework


    # 4. Formatting stock prices with Spark and the DockerOperator
    format_prices = DockerOperator(
        task_id='format_prices',
        image='airflow/stock-app',
        container_name='format_prices',
        api_version='auto',
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS': '{{task_instance.xcom_pull(task_ids="store_prices")}}'
        }
    )

    get_formatted_csv = PythonOperator(
        task_id='get_formatted_csv',
        python_callable=_get_formatted_csv,
        op_kwargs={
            'path': '{{task_instance.xcom_pull(task_ids="store_prices")}}'
        }
    )

    # 5. The best way to load files into data warehouses with Postgres and Astro SDK
    # loads csv file from minio into our data warehouse (postgres)
    
    load_to_dw = aql.load_file(
        task_id='load_to_dw',
        input_file=File(path=f"s3://{BUCKET_NAME}/{{task_instance.xcom_pull(task_ids='store_prices')}}", conn_id='minio'),
        output_table=Table(
            name='stock_market',
            conn_id='postgres',
            metadata=Metadata(
                schema='public'
            )
        )
    )    






    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_to_dw
    # önce get_stock_prices (downstream) çalışır sonra is_api_available (upstream)

stock_market() 



#https://query1.finance.yahoo.com/v8/finance/chart/aapl?metrics=high&interval=1d&range=1y

