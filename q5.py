import os
import sys

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

ENV = Variable.get("env")
IS_PROD = True if ENV == "production" else False

DEFAULT_ARGS = {
    "owner": "Ricky",
    "email": "foobar@foobar.com",
    "start_date": datetime(2021, 3, 12, 17),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
    "email_on_failure": IS_PROD
}


with DAG(
        "gojek-assignment",
        default_args = DEFAULT_ARGS,
        schedule_interval="0 22 * * *",
        max_active_runs     = 1,
        catchup             = True,
        dagrun_timeout      = timedelta(minutes=90)
) as dag:
    q1_job = BashOperator(
        task_id="gojek-assignment_q1",
        bash_command = "python q1.py",
        dag=dag,
        execution_timeout=timedelta(minutes=60),
        retry_delay=timedelta(minutes=10),
        retries=2
    )

    q2_job = BashOperator(
        task_id="gojek-assignment_q2",
        bash_command = "python q2.py",
        dag=dag,
        execution_timeout=timedelta(minutes=60),
        retry_delay=timedelta(minutes=10),
        retries=2
    )

    q1_job >> q2_job
