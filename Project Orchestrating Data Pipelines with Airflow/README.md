# Summary of project

A Startup called Sparkify needs to analyze the data they've been gathering on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs people are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

An ETL Pileline was created in order to allow sparkify analyze their data. This Pipeline takes data from the logs, processes it and stores it in a data lake ready for analysis.

# How to run this pipeline
Clone this project
Install Apache Airflow using

 ```pip install airflow```

Open the airflow webserver running on port 3000 by default
Execute the DAG

# Files in the repository

* **[airflow](airflow)**:  Folder containing the airflow DAGs/plugins used by the airflow webserver.
* **[airflow/dags/s3_to_redshift.py](airflow/dags/s3_to_redshift.py)**: DAG file containing steps of the pipeline.
* **[airflow/plugins/operators/](airflow/plugins/operators)**: Operators which are called inside the Airflow DAG. These are python classes to outsource and build generic functions as modules of the DAG.


# The database schema design and ETL pipeline.

To allow Sparkify analyze their data, a data lake was created allow sparkify acheive this purpose. As an intermediate step, an RDMS schema was built was is populated with data by the ETL Pipeline. The schema allows Sparkify view user behavoir over several dimensions while storing facts in fact tables. With this schema, Spariky can analyze user behavouir along the dimensions of songs,artists and most importanlt time, which has even been bucketized.

* **Fact Table**: songplays
* **Dimension Tables**: users, songs, artists and time

# Dataset used


* **Song data**: ```s3://udacity-dend/song_data```
* **Log data**: ```s3://udacity-dend/log_data```
