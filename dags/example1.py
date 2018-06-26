from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import requests
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'retries':2
}


dag = DAG(
    'simple_example', default_args=default_args, schedule_interval='@once')


def examplePrint(**kwargs):
	print("Simple dag(task)")
	print("these is dependent on previous task")

ex="""
echo "hello vlba"
"""
t1=BashOperator(
    task_id='bash_operator',
    bash_command=ex,
    dag=dag)

t2=PythonOperator(
    task_id='python_operator',
    python_callable=examplePrint,
    provide_context=True,	
    dag=dag)





t2.set_upstream(t1)



