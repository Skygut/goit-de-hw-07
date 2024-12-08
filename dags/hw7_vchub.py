from airflow.sensors.sql_sensor import SqlSensor
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.utils.trigger_rule import TriggerRule
import random
import mysql.connector

# Назва з'єднання з базою даних MySQL
connection_name = "goit_mysql_db_chub"


# Функції для завдань
def choose_medal():
    # Логіка для вибору медалі
    return random.choice(["calc_Bronze", "calc_Silver", "calc_Gold"])


def process_medal_task(**kwargs):
    # Отримання значення з попереднього завдання
    ti = kwargs["ti"]
    selected_task = ti.xcom_pull(task_ids="pick_medal")
    if selected_task in ["calc_Bronze", "calc_Silver", "calc_Gold"]:
        return selected_task
    else:
        raise ValueError("Invalid task selected")


def generate_delay():
    import time

    time.sleep(35)  # Затримка для перевірки сенсора


# Налаштування DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "vchub_hw7_dag7",
    default_args=default_args,
    description="Vchub DAG for medal selection and counting",
    schedule_interval=None,
    start_date=datetime(2024, 12, 8),
    catchup=False,
    tags=["vchub"],
)

# Завдання DAG
create_table = MySqlOperator(
    task_id="create_table",
    sql="CREATE TABLE IF NOT EXISTS neo_data.vchub_medals_counts (id INT AUTO_INCREMENT PRIMARY KEY, type VARCHAR(50), count INT, created_at DATETIME);",
    mysql_conn_id=connection_name,
    dag=dag,
)

pick_medal = PythonOperator(
    task_id="pick_medal",
    python_callable=choose_medal,
    dag=dag,
)

pick_medal_task = BranchPythonOperator(
    task_id="pick_medal_task",
    python_callable=process_medal_task,
    provide_context=True,
    dag=dag,
)

calc_gold = MySqlOperator(
    task_id="calc_Gold",
    sql="""
        INSERT INTO neo_data.vchub_medals_counts (type, count, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
    """,
    mysql_conn_id=connection_name,
    dag=dag,
)

calc_silver = MySqlOperator(
    task_id="calc_Silver",
    sql="""
        INSERT INTO neo_data.vchub_medals_counts (type, count, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
    """,
    mysql_conn_id=connection_name,
    dag=dag,
)

calc_bronze = MySqlOperator(
    task_id="calc_Bronze",
    sql="""
        INSERT INTO neo_data.vchub_medals_counts (type, count, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
    """,
    mysql_conn_id=connection_name,
    dag=dag,
)

generate_delay_task = PythonOperator(
    task_id="generate_delay",
    python_callable=generate_delay,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

check_for_correctness = SqlSensor(
    task_id="check_for_correctness",
    conn_id=connection_name,
    sql="SELECT COUNT(*) FROM neo_data.vchub_medals_counts WHERE created_at >= NOW() - INTERVAL 30 SECOND;",
    mode="poke",
    poke_interval=5,
    timeout=600,
)

# Зв'язки між завданнями
create_table >> pick_medal
pick_medal >> pick_medal_task
pick_medal_task >> [calc_gold, calc_silver, calc_bronze]
calc_gold >> generate_delay_task
calc_silver >> generate_delay_task
calc_bronze >> generate_delay_task
generate_delay_task >> check_for_correctness
