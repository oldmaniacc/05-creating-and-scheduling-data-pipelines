from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Function with passing parameter
with DAG( 
   
    # Pipeline name
    'my_dag',

    # Schedule Start date
    start_date = timezone.datetime(2022,10,8),

    # # Not yet auto-execution >>> need manual perform
    schedule = None,

    # CRON : run schedule, start execution since open the computer
    # https://crontab.guru/
    # all * >>> run every minute
    # schedule = '* * * * *',

    # tags for filtering in AirFlow UI
    tags = ['workshop'],
):
    # Task with nothing (empty)
    t1 = EmptyOperator(task_id='t1')

    # Task on BashOperator
    echo_hello = BashOperator(
        # task name
        task_id='echo_hello',
        # do the task
        bash_command='echo "hello"',
    )

    # function to be called by PythonOperator
    def _print_hey():
        print('Hey!')

    # Task on PythonOperator
    print_hey = PythonOperator(
        # task name
        task_id='print_hey',
        # do the task >>> call function
        python_callable=_print_hey,
    )

    # Task with nothing (empty)
    t2 = EmptyOperator(task_id='t2')

    # Order dependency flow in sequential
    # t1 >> echo_hello >> print_hey >> t2 

    # Order in parallel
    # t1 >> echo_hello 
    # t1 >> print_hey
    # echo_hello >> t2 
    # print_hey >> t2 

    # or
    t1 >> echo_hello >> t2
    t1 >> print_hey >> t2

    # Order in parallel, t2 will be run only after both 2 tasks finish
    # t1 >> [echo_hello, print_hey] >> t2 