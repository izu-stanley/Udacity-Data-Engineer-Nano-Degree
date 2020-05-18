# Summary of project

Startup called Sparkify needs to evaluate the data they have gathered on their new music listening platform on songs and user behavior. The analytics department is especially keen to learn what music people listen to. They don't currently have an easy way to query their data, which resides in a JSON log directory on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


A Relational Database Schema for AWS Redshift was developed to allow Sparkify to analyze their data, which can be filled in with an ETL pipeline.

# How to run the python scripts

To build the database tables and operate the ETL pipeline, the following two files must be run in the order they are shown below

To create tables in Redshift:
```bash
python create_tables.py
```
To fill tables via ETL:
```bash
python etl.py
```

# Files in the repository


* **[create_tables.py](create_tables.py)**: Script to execute SQL Statements for deleting and creating database and tables
* **[sql_queries.py](sql_queries.py)**: Script containing SQL Statements used by create_tables and etl scripts
* **[etl.py](etl.py)**: Script to pull out the needed information from Song and Log data residing in S3 for parsing and inserting to Redshift 

# The purpose of this database

Using a database makes it easier to analyze the data. By using SQL and designing our database with the star schema, joins and aggregations, the data can be very easily analyszed. In order to design and build for scale, we are using Amazon Redshift which is a distributed database.

* **Fact Table**: songplays
* **Dimension Tables**: users, songs, artists and time.

# Dataset

The dataset used is the dataset provided by Udacity in s3 buckets hosted on AWS

* **Song data**: ```s3://udacity-dend/song_data```
* **Log data**: ```s3://udacity-dend/log_data```
