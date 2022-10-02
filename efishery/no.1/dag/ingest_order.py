from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import \
    BigQueryExecuteQueryOperator
from datetime import datetime, timedelta
from dags.python_scripts.airflow_callback import callback

# Airflow Config
default_args = {
    'owner': 'farhan@data.com',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 31, 17),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': [],
    'retries': 5,
    'retry_delay': timedelta(seconds=300),
    'on_failure_callback': callback
}

dag_id = "ingest_order"
dag = DAG(
    dag_id,
    default_args=default_args,
    schedule_interval='0 24 * * *',
    catchup=False,
    concurrency=50,
    max_active_runs=1
)

project_id = Variable.get('PROJECT_ID')
location = Variable.get('PROJECT_LOCATION')
target_project_id = Variable.get('TARGET_PROJECT_ID')

start_date = (
    '{{ execution_date'
    '.in_timezone("Asia/Jakarta").start_of("day")'
    '.in_timezone("UTC").date() }}'
)
end_date = (
    '{{ execution_date'
    '.in_timezone("Asia/Jakarta").add(days=1).start_of("day")'
    '.in_timezone("UTC").date() }}'
)
run_date_ds_nodash = (
    '{{ execution_date'
    '.in_timezone("Asia/Jakarta").add(days=1).start_of("day")'
    '.in_timezone("UTC").date().format("YYYYMMDD") }}'
)
table_name = "dim_customer"
target_table = f"{target_project_id}.source.{table_name}"
op = BigQueryExecuteQueryOperator(
    task_id='dim_customer',
    sql='sql/dim_customer.sql',
    destination_dataset_table=f'{target_table}${run_date_ds_nodash}',
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    schema_update_options=['ALLOW_FIELD_ADDITION'],
    query_params=[
        {
            'name': 'start_date',
            'parameterType': {'type': 'DATE'},
            'parameterValue': {'value': start_date}
        },
        {
            'name': 'end_date',
            'parameterType': {'type': 'DATE'},
            'parameterValue': {'value': end_date}
        }
    ],
    use_legacy_sql=False,
    location=location,
    dag=dag
)
dim_customer_op = op

table_name = "dim_date"
target_table = f"{target_project_id}.source.{table_name}"
op = BigQueryExecuteQueryOperator(
    task_id='dim_date',
    sql='sql/dim_date.sql',
    destination_dataset_table=f'{target_table}${run_date_ds_nodash}',
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    schema_update_options=['ALLOW_FIELD_ADDITION'],
    query_params=[
        {
            'name': 'start_date',
            'parameterType': {'type': 'DATE'},
            'parameterValue': {'value': start_date}
        },
        {
            'name': 'end_date',
            'parameterType': {'type': 'DATE'},
            'parameterValue': {'value': end_date}
        }
    ],
    use_legacy_sql=False,
    location=location,
    dag=dag
)
dim_date_op = op

table_name = "fact_order_accumulating"
target_table = f"{target_project_id}.source.{table_name}"
op = BigQueryExecuteQueryOperator(
    task_id='fact_order_accumulating',
    sql='sql/fact_order_accumulating.sql',
    destination_dataset_table=f'{target_table}${run_date_ds_nodash}',
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    schema_update_options=['ALLOW_FIELD_ADDITION'],
    query_params=[
        {
            'name': 'start_date',
            'parameterType': {'type': 'DATE'},
            'parameterValue': {'value': start_date}
        },
        {
            'name': 'end_date',
            'parameterType': {'type': 'DATE'},
            'parameterValue': {'value': end_date}
        }
    ],
    use_legacy_sql=False,
    location=location,
    dag=dag
)
fact_order_accumulating_op = op
dim_customer_op \
    >> fact_order_accumulating_op
dim_date_op \
    >> fact_order_accumulating_op