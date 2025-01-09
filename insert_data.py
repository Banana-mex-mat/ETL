from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.configuration import conf
from airflow.models import Variable
import pandas
import os
from datetime import datetime

PATH = Variable.get("my_path")
conf.set("core", "template_searchpath", PATH)

def insert_data(table_name):
    df = pandas.read_csv(f"/files/{table_name}.csv", delimiter=";")
    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, schema="test", if_exists="append", index=False)

def insert_data2(table_name):
    file_path = os.path.join(PATH, f"{table_name}.csv")
    print(f"Используемый путь: {file_path}")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Файл не найден: {file_path}")
    
    df = pandas.read_csv(file_path, delimiter=";", encoding="cp1252")
    
    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    
    df.to_sql(table_name, engine, schema="test", if_exists="append", index=False)

default_args = {
    "owner": "irina",
    "start_date": datetime(2024, 2, 25),
    "retries": 2
} 

with DAG(
    "insert_data",
    default_args=default_args,
    description="Загрузка данных в test",
    catchup=False,
    template_searchpath = [PATH],
    # template_searchpath="",
    schedule="0 0 * * *" 
) as dag:
    start = DummyOperator( 
        task_id = "start" )
    
    ft_balance_f = PythonOperator(
        task_id="ft_balance_f",
        python_callable=insert_data,
        op_kwargs={"table_name" : "ft_balance_f"}
    )

    ft_posting_f = PythonOperator(
        task_id="ft_posting_f",
        python_callable=insert_data,
        op_kwargs={"table_name" : "ft_posting_f"}
    )
    md_account_d = PythonOperator(
        task_id="md_account_d",
        python_callable=insert_data,
        op_kwargs={"table_name" : "md_account_d"}
    )
    md_currency_d = PythonOperator(
        task_id="md_currency_d",
        python_callable=insert_data2,
        op_kwargs={"table_name" : "md_currency_d"}
    )
    ft_postmd_exchange_rate_d = PythonOperator(
        task_id="md_exchange_rate_d",
        python_callable=insert_data,
        op_kwargs={"table_name" : "md_exchange_rate_d"}
    )
    md_ledger_account_s = PythonOperator(
        task_id="md_ledger_account_s",
        python_callable=insert_data,
        op_kwargs={"table_name" : "md_ledger_account_s"}
    )
    end = DummyOperator(
        task_id = "end"
    )
    (
        start
        >> [ft_balance_f, ft_posting_f, md_account_d, md_currency_d, ft_postmd_exchange_rate_d, md_ledger_account_s]
        >> end
    )
