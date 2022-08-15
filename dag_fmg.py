from airflow.models import DAG
from airflow.operators.python import PythonOperator
from fmg_packages.main import pull_data_from_storage, cleanse_data, resulting_data, extract_results, pull_data, \
    upload_file
from datetime import datetime, timedelta

start_date = datetime(2022, 7, 13)
DAG_ID = 'fortune_mine_games.v.0.1'

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': start_date,
    'email': ['efesabanoglu@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'pool': 'default_pool',
    'execution_timeout': timedelta(hours=1)
}

EXECUTION_DATE_FORMAT = '%Y-%m-%d'

with DAG(
        dag_id=DAG_ID,
        catchup=False,
        schedule_interval='00 17 * * *',
        default_args=default_args,
        is_paused_upon_creation=True,
        max_active_runs=1) as dag:
    EXECUTION_DATE = '{{ execution_date }}'

    # create_bucket = PythonOperator(
    #     task_id='create_bucket_in_storage',
    #     dag=dag,
    #     python_callable=create_bucket(bucket_name=BUCKET_NAME),
    #     provide_context=True
    # )

    pull_data_from_storage = PythonOperator(
        task_id='pull_data_from_storage',
        dag=dag,
        python_callable=pull_data(),
        op_kwargs={'EXECUTION_DATE': EXECUTION_DATE},
        provide_context=True
    )

    cleanse_data = PythonOperator(
        task_id='cleaning_data',
        dag=dag,
        python_callable=cleanse_data(),
        op_kwargs={'EXECUTION_DATE': EXECUTION_DATE},
        provide_context=True
    )

    resulting_data = PythonOperator(
        task_id='resulting_data',
        dag=dag,
        python_callable=resulting_data(),
        op_kwargs={'EXECUTION_DATE': EXECUTION_DATE},
        provide_context=True
    )

    extract_results = PythonOperator(
        task_id='extract_results',
        dag=dag,
        python_callable=extract_results(),
        op_kwargs={'EXECUTION_DATE': EXECUTION_DATE},
        provide_context=True
    )

    pull_data_from_storage >> cleanse_data >> resulting_data >> extract_results

if __name__ == "__main__":
    _start_date = datetime(2022, 8, 11)
    #execution_ts = _start_date.strftime('%Y-%m-%dT00:00:00+00:00')
    #create_bucket(RESULTS_BUCKET)
    #upload_file(BUCKET_NAME, SOURCE_FILE_NAME, DESTINATION_BLOB_NAME)
    #pull_data_from_storage(start_date=_start_date)
    #cleanse_data()
    #resulting_data()
    #extract_results()