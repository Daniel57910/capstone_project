from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from lib.s3_to_gzip import s3_to_gzip
from sql.create_tables import table_commands

default_args = {
  'owner': 'daniel miller/Open Source',
  'start_date': datetime(2018, 1, 12),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'email_on_failure': False,
  'depends_on_past': False,
  'retries': 3
}

dag = DAG(
  'sparkify_dag', default_args=default_args, description='First Dag', schedule_interval='@hourly', catchup=False)

