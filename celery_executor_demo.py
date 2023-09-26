
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time

def hello_function():
	print('Hello, this is the first task of the DAG')
	time.sleep(5)

def last_function():
	print('DAG run is done.')

def sleeping_function():
	print("Sleeping for 5 seconds")
	time.sleep(5)

with DAG(
		dag_id="celery_executor_demo",
		start_date=datetime(2021,1,1),
		schedule_interval="@hourly",
		catchup=False) as dag:

		task1=PythonOperator(
		task_id="start",
		python_callable=hello_function
		)

		task2_1=PythonOperator(
		task_id="sleepy_21",
		python_callable=sleeping_function
		)

		task2_2=PythonOperator(
		task_id="sleepy_22",
		python_callable=sleeping_function
		)

		task2_3=PythonOperator(
		task_id="sleepy_23",
		python_callable=sleeping_function
		)

		task3_1=PythonOperator(
		task_id="sleepy_31",
		python_callable=sleeping_function
		)

		task3_2=PythonOperator(
		task_id="sleepy_32",
		python_callable=sleeping_function
		)

		task3_3=PythonOperator(
		task_id="sleepy_33",
		python_callable=sleeping_function
		)

		task3_4=PythonOperator(
		task_id="sleepy_34",
		python_callable=sleeping_function
		)

		task3_5=PythonOperator(
		task_id="sleepy_35",
		python_callable=sleeping_function
		)

		task3_6=PythonOperator(
		task_id="sleepy_36",
		python_callable=sleeping_function
		)

		task4_1=PythonOperator(
		task_id="sleepy_41",
		python_callable=sleeping_function
		)

		task4_2=PythonOperator(
		task_id="sleepy_42",
		python_callable=sleeping_function
		)

		task5=PythonOperator(
		task_id="sleepy_5",
		python_callable=sleeping_function
		)

		task6=PythonOperator(
		task_id="end",
		python_callable=last_function
		)

task1>>[task2_1,task2_2, task2_3]
task2_1>>[task3_1, task3_2]
task2_2>>[task3_3, task3_4]
task2_3>>[task3_5, task3_6]
[task3_1, task3_2, task3_3]>>task4_1
[task3_4, task3_5, task3_6]>>task4_2
[task4_1, task4_2]>>task5>>task6
celery_executor_demo.py
Displaying celery_executor_demo.py.