import datetime as dt
import os
import sys
from airflow.models import DAG
from airflow.operators.python import PythonOperator


path = os.path.expanduser('~/PycharmProjects/SberAuto')
# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)

from modules.pipeline_ga_hits_json_to_sql import pipeline
from modules.pipeline_ga_sessions_json_to_sql import pipeline2

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 12, 25),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='Insert new files to MySQL',
        schedule_interval="00 15 * * *",
        default_args=args,
) as dag:
    pipeline = PythonOperator(
        task_id='pipeline_ga_hits',
        python_callable=pipeline,
        dag=dag,
    )
    pipeline2 = PythonOperator(
        task_id='pipeline_ga_session',
        python_callable=pipeline2,
        dag=dag,
    )
    pipeline >> pipeline2
