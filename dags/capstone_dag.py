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
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'email_on_failure': False,
  'depends_on_past': False,
}

dag = DAG('capstone_project_dag', default_args=default_args, description='capstone project dag', schedule_interval='@monthly', catchup=False)

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

generate_city_and_temperature_dimension_data = BashOperator(
  task_id='generate_city_and_temperature_dimension_files',
  bash_command =SOURCE_VIRTUAL_ENV + f'spark-submit {SPARK_PROJECT_PATH}/load_city_and_temperature_dimensions.py',
  dag=dag
)

generate_immigration_dimension_data = BashOperator(
  task_id='generate_immigration_dimension_files',
  bash_command =SOURCE_VIRTUAL_ENV + f'spark-submit --packages saurfang:spark-sas7bdat:2.0.0-s_2.10 {SPARK_PROJECT_PATH}/aggregate_immigration_data.py',
  dag=dag
)

generate_immigration_demographic_summary = BashOperator(
  task_id='generate_fact_summary_data_of_immigration_temperature_demographics',
  bash_command =SOURCE_VIRTUAL_ENV + f'spark-submit {SPARK_PROJECT_PATH}/create_immigration_demography_summary.py',
  dag=dag
)

dimension_data_quality_check = BashOperator(
  task_id='dimension_data_quality_check',
  bash_command =SOURCE_VIRTUAL_ENV + f'spark-submit {SPARK_PROJECT_PATH}/dimension_data_quality_check.py',
  dag=dag
)

fact_data_quality_check = BashOperator(
  task_id='fact_data_quality_check',
  bash_command =SOURCE_VIRTUAL_ENV + f'spark-submit {SPARK_PROJECT_PATH}/fact_data_quality_check.py',
  dag=dag
)

sync_dataset >> unzip_dataset >> [generate_city_and_temperature_dimension_data, generate_immigration_dimension_data] >> dimension_data_quality_check 
dimension_data_quality_check >> generate_immigration_demographic_summary >> fact_data_quality_check