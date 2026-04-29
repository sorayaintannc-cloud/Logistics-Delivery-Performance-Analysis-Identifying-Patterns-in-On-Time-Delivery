import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port="5434",
    database="db_m3",
    user="airflow",
    password="airflow"
)

cur = conn.cursor()

with open("P2M3_soraya_intan_data_raw.csv", "r") as f:
    cur.copy_expert("""
        COPY table_m3
        FROM STDIN
        WITH CSV HEADER
    """, f)

conn.commit()
cur.close()
conn.close()

print("SUCCESS INSERT RAW DATA")