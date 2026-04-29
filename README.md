# Logistics Delivery Performance Analysis: Identifying Patterns in On-Time Delivery

## Repository Outline
```
1. README.md - Berisi deskripsi umum project, tujuan, metode, dan teknologi yang digunakan.
2. soraya_intan_DAG.py - Script DAG Apache Airflow untuk automasi proses ETL (fetch, cleaning, load ke Elasticsearch).
3. soraya_intan_data_clean.csv - Dataset yang telah melalui proses data cleaning dan siap digunakan untuk analisis.
4. soraya_intan_data_raw.csv - Dataset mentah sebelum dilakukan proses pembersihan data.
5. soraya_intan_ddl.txt - Berisi syntax DDL dan DML untuk pembuatan tabel dan insert data ke PostgreSQL.
6. soraya_intan_GX.ipynb - Notebook untuk validasi data menggunakan Great Expectations.
7. soraya_intan_DAG_graph.png - Visualisasi graph DAG yang menunjukkan alur pipeline Airflow.
8. Docker-compose.yml - Konfigurasi Docker untuk menjalankan seluruh services (PostgreSQL, Airflow, Elasticsearch, dan Kibana)
9. Folder images - Berisi screenshot visualisasi dashboard Kibana beserta insight yang dihasilkan.
```

## Problem Background
Dalam industri e-commerce, ketepatan waktu pengiriman merupakan salah satu faktor utama yang mempengaruhi kepuasan pelanggan dan efisiensi operasional. Keterlambatan pengiriman dapat berdampak langsung pada kualitas layanan, sehingga penting bagi perusahaan untuk memastikan sistem distribusi berjalan secara optimal.

Namun, penyebab keterlambatan tidak selalu jelas dan dapat dipengaruhi oleh berbagai faktor, seperti metode pengiriman, distribusi beban antar warehouse, serta strategi bisnis seperti pemberian diskon. Dalam banyak kasus, sulit untuk mengetahui apakah keterlambatan disebabkan oleh faktor tertentu atau merupakan hasil dari kondisi sistem secara keseluruhan.

Oleh karena itu, diperlukan analisis data untuk memahami pola yang terjadi dalam proses distribusi, serta mengevaluasi apakah terdapat faktor yang berkontribusi signifikan terhadap performa pengiriman.

## Project Output
Output dari project ini adalah:

Dashboard menggunakan Kibana untuk visualisasi data logistik dan insight mengenai:
- Performa pengiriman (on-time vs delay)
- Distribusi metode pengiriman
- Beban kerja warehouse
- Pola kepuasan pelanggan
- Strategi pemberian diskon

## Data
Dataset yang digunakan merupakan data logistik pengiriman dengan karakteristik:

- Jumlah data: ±10.999 baris

Data berisi informasi seperti:
- Mode of shipment
- Warehouse block
- Discount offered
- Customer rating
- Status ketepatan waktu pengiriman (reachedontime_yn)
- Target utama:
- Reachedontime_yn (1 = tepat waktu, 0 = terlambat)

## Method
Project ini menggunakan pendekatan pipeline data end-to-end, dimulai dari memasukkan data mentah ke PostgreSQL, kemudian dilakukan proses ekstraksi dan data cleaning menggunakan Python. Data yang telah bersih divalidasi menggunakan Great Expectations untuk memastikan kualitasnya, lalu dimuat ke Elasticsearch. Seluruh proses diotomatisasi menggunakan Apache Airflow, dan hasil akhirnya divisualisasikan di Kibana untuk menghasilkan insight bisnis.

## Stacks
Project ini menggunakan teknologi dan tools berikut:

- Python (untuk data processing)
- PostgreSQL (data storage)
- Apache Airflow (ETL pipeline orchestration)
- Elasticsearch (data indexing)
- Kibana (dashboard & visualisasi)
- Docker (containerization)

## Reference
Dataset: https://www.kaggle.com/datasets/prachi13/customer-analytics

Dashboard Kibana: Folder image

Dokumentasi:
https://www.elastic.co/kibana

https://airflow.apache.org/

---