# Summary 

A Startup called Sparkify needs to analyze the data they've been gathering on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs people are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

An ETL Pileline was created in order to allow sparkify analyze their data.
This Pipeline takes data from the logs, processes it and stores it in a data lake ready for analysis.


# How to execute scripts

To run the ETL pipeline, execute the script below. Take care to ensure that you have given the script executable permissions if you are on Unix based systems:


```bash
python3 etl.py
```

# Files in this repo

* **[etl.py](etl.py)**: Script that extracts required information from logs stored in s3 buckets.
The actual processing is done using Spark and can run on any spark cluster of your choosing, be it on prem or with any cloud provider.


# The schema design and pipeline.

To allow Sparkify analyze their data, a data lake was created allow sparkify acheive this purpose. As an intermediate step, an RDMS schema was built was is populated with data by the ETL Pipeline.
The schema allows Sparkify view user behavoir over several dimensions while storing facts in fact tables. 
With this schema, Spariky can analyze user behavouir along the dimensions of songs,artists and most importanlt time, which has even been bucketized.

* **Fact Table**: songplays
* **Dimension Tables**: users, songs, artists and time.

# Dataset 

* **Song data**: ```s3://udacity-dend/song_data```
* **Log data**: ```s3://udacity-dend/log_data```
