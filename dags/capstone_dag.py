from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from datetime import datetime, timedelta

SPARK_PROJECT_PATH='/home/hadoop/spark'
DATASET_PATH = '/mnt1'
SPARK_SOURCE_PATH='mnt1/data'
DIMENSION_PATH='/mnt1/dimensions'
SOURCE_VIRTUAL_ENV= 'source /home/hadoop/capstone-venv/bin/activate && '

default_args = {
  'owner': 'daniel miller/Open Source',
  'start_date': datetime(2018, 1, 12),
  # 'retries': 1,
  # # 'retry_delay': timedelta(minutes=5),
  'email_on_failure': False,
  'depends_on_past': False,
  # 'retries': 3
}

dag = DAG(
  'sparkify_dag', default_args=default_args, description='First Dag', schedule_interval='@hourly', catchup=False)

sync_dataset= BashOperator(
  task_id='sync_dataset',
  bash_command='aws s3 sync s3://capstone-dmiller-bucket /mnt1',
  dag=dag
)

unzip_dataset = BashOperator(
  task_id='unzip_dataset',
  bash_command='unzip /mnt1/data.zip -d /mnt1/',
  dag=dag
)

create_demographic_dimension = BashOperator(
  task_id='load_demographic_summary_of_states_into_rdd',
  bash_command =SOURCE_VIRTUAL_ENV + f'spark-submit {SPARK_PROJECT_PATH}/load_city_and_temperature_dimensions.py',
  dag=dag
)

# create_weather_dimension = BashOperator(
#   task_id='lo'
# )

sync_dataset >> unzip_dataset >> create_demographic_dimension