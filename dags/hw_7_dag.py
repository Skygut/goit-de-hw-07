from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

# Назва з'єднання з MySQL
connection_name = "goit_mysql_db_chub"

default_args = {
    "start_date": datetime(2024, 12, 7),
}

with DAG(
    dag_id="vchub_medal_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=["vchub"],
) as dag:

    # Завдання 0: Створення бази даних
    create_database = MySqlOperator(
        task_id="create_database",
        mysql_conn_id=connection_name,
        sql="CREATE DATABASE IF NOT EXISTS vchub;",
    )

    # Завдання 1: Використання бази даних
    use_database = MySqlOperator(
        task_id="use_database", mysql_conn_id=connection_name, sql="USE vchub;"
    )

    # Завдання 2: Видалення таблиці, якщо існує
    drop_table = MySqlOperator(
        task_id="drop_table",
        mysql_conn_id=connection_name,
        sql="DROP TABLE IF EXISTS vchub_medals;",
    )

    # Завдання 3: Створення таблиці
    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS vchub_medals (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    # Завдання 4: Наповнення таблиці
    populate_table = MySqlOperator(
        task_id="populate_table",
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO vchub_medals (medal_type, count, created_at) VALUES
        ('Gold', 820, CURRENT_TIMESTAMP),
        ('Silver', 2120, CURRENT_TIMESTAMP),
        ('Bronze', 15, CURRENT_TIMESTAMP),
        ('Gold', 255, CURRENT_TIMESTAMP),
        ('Silver', 25, CURRENT_TIMESTAMP),
        ('Bronze', 3340, CURRENT_TIMESTAMP),
        ('Gold', 83, CURRENT_TIMESTAMP),
        ('Silver', 148, CURRENT_TIMESTAMP),
        ('Bronze', 162, CURRENT_TIMESTAMP),
        ('Gold', 174, CURRENT_TIMESTAMP);
        """,
    )

    # Завдання 5: Примусове завершення DAG як успішного
    force_success = DummyOperator(task_id="force_success", trigger_rule="all_failed")

    # Зв'язок між завданнями
    (
        create_database
        >> use_database
        >> drop_table
        >> create_table
        >> populate_table
        >> force_success
    )
