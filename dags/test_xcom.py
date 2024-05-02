from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator


def _push(**context):
    # ti = context['ti']
    # ti.xcom_push(key = 'name', value = 'chin')

    # return 'Chin'
    return ["Kan", "Chin", "Lyn", "Nin"]

def _pull(**context):
    ti = context['ti']
    ds = context['ds']
    # name = ti.xcom_pull(task_ids = 'push', key = 'name')

    # name = ti.xcom_pull(task_ids = 'push', key = 'return_value')
    # print(f'Hello {name}')

    names = ti.xcom_pull(task_ids = 'push', key = 'return_value')
    for n in names:
        print(f'Hello {n} on {ds}')


with DAG(
    'test_xcom',
    start_date = timezone.datetime(2022, 10, 15),
    schedule = '@daily',
    tags = ['workshop'],
    catchup = False,
) as dag:

    push = PythonOperator(
        task_id = 'push',
        python_callable = _push,
    )

    pull = PythonOperator(
        task_id = 'pull',
        python_callable = _pull,
    )