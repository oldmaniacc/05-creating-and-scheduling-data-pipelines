import logging

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def _say_hello(name = '', **context): # ** mean dict variable
    print(context)
    datestamp = context['ds']
    # ds_nodash = context['ds_nodash'] # pass var from call function
    print(f'Hello! {name} on {datestamp}')

def _print_log_messages():
    logging.info('Hello from Log')


with DAG(
    'my_dag2',
    start_date = timezone.datetime(2022, 10, 15),
    schedule = '*/30 * * * *',
    tags = ['workshop'],
    catchup = False,
) as dag:

    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id = 'end')

    echo_hello = BashOperator(
        task_id = 'echo_hello',
        bash_command = 'echo "hello" on {{ ds }}',
    )

    say_hello = PythonOperator(
        task_id = 'say_hello',
        python_callable = _say_hello,
        op_kwargs = {
            'name' : 'chin {{ ds_nodash }}'
        },
    )

    print_log_messages = PythonOperator(
        task_id = 'print_log_messages',
        python_callable = _print_log_messages,
    )

    # start >> echo_hello >> say_hello >> print_log_messages >> end
    start >> echo_hello >> [say_hello, print_log_messages] >> end