'''
=================================================
Milestone 3

Nama  : Soraya Intan
Batch : FTDS-HCK-038

Program ini dibuat untuk melakukan automasi pengambilan data dari PostgreSQL,
melakukan data cleaning, menyimpan data clean ke CSV, dan mengirim data
ke Elasticsearch.
=================================================
'''

import datetime as dt
import re
import pandas as pd
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch


def fetch_from_postgresql():
    '''
    Fungsi ini digunakan untuk mengambil seluruh data dari table_m3
    yang berada di PostgreSQL, kemudian menyimpannya sementara
    sebagai CSV raw hasil fetch.
    '''

    conn = psycopg2.connect(
        host='postgres_m3',
        database='db_m3',
        user='airflow',
        password='airflow',
        port=5432
    )

    query = 'SELECT * FROM table_m3'
    df = pd.read_sql(query, conn)

    df.to_csv('/opt/airflow/dags/P2M3_soraya_intan_data_fetched.csv', index=False)

    conn.close()


def clean_data():
    '''
    Fungsi ini digunakan untuk membersihkan data sesuai instruksi Milestone 3:
    menghapus duplikat, normalisasi nama kolom, dan handling missing values.
    Hasil akhir disimpan sebagai CSV clean.
    '''

    df = pd.read_csv('/opt/airflow/dags/P2M3_soraya_intan_data_fetched.csv')

    # Remove duplicate data
    df = df.drop_duplicates()

    # Normalize column names
    df.columns = [
        re.sub(r'[^a-zA-Z0-9_]', '', col.strip().lower().replace(' ', '_'))
        for col in df.columns
    ]

    # Handling missing values
    for col in df.columns:
        if df[col].isnull().sum() > 0:
            if df[col].dtype == 'object':
                df[col] = df[col].fillna(df[col].mode()[0])
            else:
                df[col] = df[col].fillna(df[col].median())

    df.to_csv('/opt/airflow/dags/P2M3_soraya_intan_data_clean.csv', index=False)


def post_to_elasticsearch():
    '''
    Fungsi ini digunakan untuk memasukkan data clean dari CSV
    ke Elasticsearch agar dapat divisualisasikan melalui Kibana.
    '''

    es = Elasticsearch(hosts=[{"host": "elasticsearch_m3", "port": 9200}])
    df = pd.read_csv('/opt/airflow/dags/P2M3_soraya_intan_data_clean.csv')

    for i, row in df.iterrows():
        doc = row.to_dict()
        es.index(index='p2m3_soraya_intan', id=i + 1, body=doc)


default_args = {
    'owner': 'soraya_intan',
    'start_date': dt.datetime(2024, 11, 1, 9, 10),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}


with DAG(
    dag_id='P2M3_soraya_intan_DAG',
    default_args=default_args,
    schedule_interval='10,20,30 9 * * 6',
    catchup=False
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_from_postgresql',
        python_callable=fetch_from_postgresql
    )

    clean_task = PythonOperator(
        task_id='data_cleaning',
        python_callable=clean_data
    )

    post_task = PythonOperator(
        task_id='post_to_elasticsearch',
        python_callable=post_to_elasticsearch
    )

    fetch_task >> clean_task >> post_task