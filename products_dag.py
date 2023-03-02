from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime

default_args = {
                'owner': 'breno',
                'depends_on_past': False,
                'email': ['brentchav@gmail.com'],
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 10,
                'retry_delay': timedelta(minutes=1)
                }


dag = DAG(
            dag_id='products_dag',
            start_date=datetime(year=2023, month=1, day=31, hour=9),
            schedule="30 */12 * * *",
            default_args=default_args,
            catchup=False
            )


task1 = BashOperator(
                     task_id='get_marcy_prod',
                     bash_command='python /home/brenoteix/repos/airflow/dags/src/get_marcy_prod.py',
                     dag=dag                 
                    )


task2 = BashOperator(task_id='get_hm_prod',
                    bash_command='python /home/brenoteix/repos/airflow/dags/src/get_hm_prod.py',
                    dag=dag)


task3 = BashOperator(task_id='insert_data',
                    bash_command='python /home/brenoteix/repos/airflow/dags/src/insert_data.py'
                    )


task1 >> task2 >> task3