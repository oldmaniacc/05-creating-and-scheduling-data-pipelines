import os
import glob
from typing import List
import json

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


table_create_repo = """
    CREATE TABLE IF NOT EXISTS Repo (
        repo_id BIGINT NOT NULL,
        repo_name VARCHAR(100) NOT NULL,
        repo_url VARCHAR(150) NOT NULL,
        PRIMARY KEY (repo_id)
    );
"""
table_create_org = """
    CREATE TABLE IF NOT EXISTS Org (
        org_id BIGINT NOT NULL,
        org_login VARCHAR(50) NOT NULL,
        org_gravatar_id VARCHAR(50),
        org_url VARCHAR(100) NOT NULL,
        org_avatar_url VARCHAR(100) NOT NULL,
        PRIMARY KEY (org_id)
    );
"""
table_create_actor = """
    CREATE TABLE IF NOT EXISTS Actor (
        actor_id BIGINT NOT NULL,
        actor_login VARCHAR(50) NOT NULL,
        actor_display_login VARCHAR(50) NOT NULL,
        actor_gravatar_id VARCHAR(50),
        actor_url VARCHAR(100) NOT NULL,
        actor_avatar_url VARCHAR(100) NOT NULL,
        PRIMARY KEY (actor_id)
    );
"""
table_create_commit = """
    CREATE TABLE IF NOT EXISTS Committed (
        commit_sha VARCHAR(100) NOT NULL,
        commit_email VARCHAR(100) NOT NULL,
        commit_name VARCHAR(100) NOT NULL,
        commit_url VARCHAR(200) NOT NULL,
        PRIMARY KEY (commit_sha)
    );
"""
table_create_payload = """
    CREATE TABLE IF NOT EXISTS Payload (
                payload_push_id BIGINT NOT NULL,
                payload_size BIGINT NOT NULL,
                payload_ref VARCHAR(200) NOT NULL,
                payload_commit_sha VARCHAR(100),
                PRIMARY KEY (payload_push_id),
                FOREIGN KEY (payload_commit_sha)  REFERENCES Committed (commit_sha)
    );
"""
table_create_event = """
    CREATE TABLE IF NOT EXISTS Event (
        event_id VARCHAR(20) NOT NULL,
        event_type VARCHAR(50) NOT NULL,
        event_public BOOLEAN NOT NULL,
        event_created_at TIMESTAMP NOT NULL,
        event_repo_id BIGINT NOT NULL,
        event_actor_id BIGINT NOT NULL,
        event_org_id BIGINT,
        event_payload_push_id BIGINT,
        PRIMARY KEY (event_id),
        FOREIGN KEY (event_repo_id)     REFERENCES Repo     (repo_id),
        FOREIGN KEY (event_actor_id)    REFERENCES Actor    (actor_id),
        FOREIGN KEY (event_org_id)      REFERENCES Org      (org_id),
        FOREIGN KEY (event_payload_push_id)  REFERENCES Payload  (payload_push_id)
    );
"""

create_table_queries = [table_create_repo, table_create_org, table_create_actor, table_create_commit, table_create_payload, table_create_event]

table_insert_repo    = "INSERT INTO Repo VALUES %s ON CONFLICT DO NOTHING;"
table_insert_org     = "INSERT INTO Org VALUES %s ON CONFLICT DO NOTHING;"
table_insert_actor   = "INSERT INTO Actor VALUES %s ON CONFLICT DO NOTHING;"
table_insert_commit  = "INSERT INTO Committed VALUES %s ON CONFLICT DO NOTHING;"
table_insert_payload = "INSERT INTO Payload VALUES %s ON CONFLICT DO NOTHING;"
table_insert_event   = "INSERT INTO Event VALUES %s ON CONFLICT DO NOTHING;"
table_insert_event_missOrg   = "INSERT INTO Event (event_id,event_type,event_public,event_created_at,event_repo_id,event_actor_id,event_payload_push_id) VALUES %s ON CONFLICT DO NOTHING;"
table_insert_event_missBoth   = "INSERT INTO Event (event_id,event_type,event_public,event_created_at,event_repo_id,event_actor_id) VALUES %s ON CONFLICT DO NOTHING;"


def _get_files(filepath: str) -> List[str]:
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        # files = glob.glob(os.path.join(root, "github_events.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files


def _create_tables(**context):
    hook = PostgresHook(postgres_conn_id = 'my_postgres')
    conn = hook.get_conn()
    cur = conn.cursor()

    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def _process(**context):
    hook = PostgresHook(postgres_conn_id = 'my_postgres')
    conn = hook.get_conn()
    cur = conn.cursor()

    # Get list of files from filepath
    ti = context['ti']
    all_files = ti.xcom_pull(task_ids = 'get_files', key = 'return_value')

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                sql_insert = ''

                # Insert for Repo
                val = each["repo"]["id"], each["repo"]["name"], each["repo"]["url"]
                sql_insert = table_insert_repo % str(val)
                cur.execute(sql_insert)

                # Insert for Actor
                val = each["actor"]["id"], each["actor"]["login"], each["actor"]["display_login"], each["actor"]["gravatar_id"], each["actor"]["url"], each["actor"]["avatar_url"]
                sql_insert = table_insert_actor % str(val)
                cur.execute(sql_insert)

                # Insert for Org
                try:
                    val = each["org"]["id"], each["org"]["login"], each["org"]["gravatar_id"], each["org"]["url"], each["org"]["avatar_url"]
                    sql_insert = table_insert_org % str(val)
                    cur.execute(sql_insert)
                except: pass

                # Insert for Commit
                sha = ''
                try:
                    for cmt in each["payload"]["commits"]:
                        val = cmt["sha"], cmt["author"]["email"], cmt["author"]["name"], cmt["url"]
                        sha = cmt["sha"]
                        sql_insert = table_insert_commit % str(val)
                        cur.execute(sql_insert)
                except: pass

                # Insert for Payload
                try:
                    if sha == '':
                        val = each["payload"]["push_id"], each["payload"]["size"], each["payload"]["ref"], 
                    else:
                        val = each["payload"]["push_id"], each["payload"]["size"], each["payload"]["ref"], sha
                    sql_insert = table_insert_payload % str(val)
                    cur.execute(sql_insert)
                except: pass

                # Insert for Event
                try: 
                    val = each["id"], each["type"], each["public"], each["created_at"], each["repo"]["id"], each["actor"]["id"], each["org"]["id"], each["payload"]["push_id"]
                    sql_insert = table_insert_event % str(val)
                except: 
                    try: 
                        val = each["id"], each["type"], each["public"], each["created_at"], each["repo"]["id"], each["actor"]["id"], each["org"]["id"], 
                        sql_insert = table_insert_event % str(val)
                    except: 
                        try: 
                            val = each["id"], each["type"], each["public"], each["created_at"], each["repo"]["id"], each["actor"]["id"],each["payload"]["push_id"]
                            sql_insert = table_insert_event_missOrg % str(val)
                        except: 
                            val = each["id"], each["type"], each["public"], each["created_at"], each["repo"]["id"], each["actor"]["id"]
                            sql_insert = table_insert_event_missBoth % str(val)
                cur.execute(sql_insert)
            
            conn.commit()



with DAG(
    'dag_etl',
    start_date = timezone.datetime(2022, 10, 15),
    schedule = '@daily',
    tags = ['workshop'],
    catchup = False,
) as dag:

    get_files = PythonOperator(
        task_id = 'get_files',
        python_callable = _get_files,
        op_kwargs = {
            'filepath' : '/opt/airflow/dags/data',
        }
    )

    process = PythonOperator(
        task_id = 'process',
        python_callable = _process,
    )

    create_tables = PythonOperator(
        task_id = 'create_table',
        python_callable = _create_tables,
    )

    get_files >> create_tables >> process