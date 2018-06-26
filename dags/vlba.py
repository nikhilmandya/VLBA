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
    'vlba', default_args=default_args, schedule_interval='@once')

polt=0
	
def totalpolitics(**context):
	ti=context['task_instance']
	df=pd.read_csv("/home/nikhil/airflow/script/Politics/politics.csv")
	temp=df['polarity'].sum()
	ti.xcom_push(key="total_politics",value=temp)
	print(polt)
	return polt
def totalsports(**context):
	ti=context['task_instance']
	df=pd.read_csv("/home/nikhil/airflow/script/Sports/sports.csv")
	temp=df['polarity'].sum()
	ti.xcom_push(key="total_politics",value=temp)
	
	print(polt)
	return polt
def compare(**context):
	ti=context['task_instance']
	sports=ti.xcom_pull(task_ids='total_sports')
	politics=ti.xcom_pull(task_ids='total_politics')
	if(sports>politics):
		print("sports has more positive")
	else:
		print("politics have more positive")
	
t1=BashOperator(
    task_id='write_to_sport',
    bash_command="python3 /home/nikhil/airflow/script/sports.py",
    dag=dag)

t2=BashOperator(
    task_id='write_to_politics',
    bash_command="python3 /home/nikhil/airflow/script/politics.py",
    dag=dag)



t3=BashOperator(
	task_id="calculate_sentiment_sports",
	bash_command='python3 /home/nikhil/airflow/script/sentisports.py',
	dag=dag)

t4=BashOperator(
	task_id="calculate_sentiment_politics",
	bash_command='python3 /home/nikhil/airflow/script/sentipolitics.py',
	dag=dag)
t5=PythonOperator(
	task_id="total_politics",
	python_callable=totalpolitics,
	provide_context=True,
	dag=dag)
t6=PythonOperator(
	task_id="total_sports",
	python_callable=totalsports,
	provide_context=True,
	dag=dag)
t7=PythonOperator(
	task_id="compare",
	python_callable=compare,
	provide_context=True,
	dag=dag)
t3.set_upstream(t1)
t4.set_upstream(t2)
t5.set_upstream(t4)
t6.set_upstream(t3)
t7.set_upstream(t6)
t7.set_upstream(t5)

