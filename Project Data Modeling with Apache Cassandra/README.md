# Udacity Data Engineering Project 2: Data Modelling with Apache Cassandra
## Getting Started

Run from command line:

* `python3 create_tables.py` (to create the DB tables in Cassandra)
* `python3 etl.py` (to perform ETL on the Data)

---

## Overview

This Project handles data of a music streaming startup, Sparkify. Dataset is in CSV format:

* **./event_data**: event data of service usage e.g. who listened what song, when, where, and with which client

Below, some stats about the data (results after running etl.py):

* ./event_data: 30 files (CSV files)
* ./event_data_new.csv: 8056 lines (concatenated input file for DB)
* song_in_session: 6820 rows
* artist_in_session: 6820 rows
* user_and_song: 6618 rows
* Query-1: Faithless, Music Matters (Mark Knight Dub), 495.30731201171875
* Query-2: 4 artists
* Query-3: 3 users

Project builds an ETL pipeline (Extract, Transform, Load) to create the DB and tables, fetch data from CSV files, process the data (combine data ), and insert the the data to DB. 

---

## About Database

Sparkify event database schema is modelled to answer the first set of customer questions:

* _"What artist and song was listened during certain session?"_,
* _"What was the artist, song and user during certain session?"_",
* _"What users have listened certain song?"_


### Tables

* **song_in_session**: songs and artists in a session (session_id, item_in_session, artist, song, length)
* **artist_in_session**: artist, song, and user in a session (user_id, session_id, artist, song, item_in_session, first_name, last_name)
* **user_and_song**: user listening certain song (song, user_id, first_name, last_name)

---

## Workflow

**Project has two scripts:**

* **create_tables.py**: Creates the necessary tables, dropping them if they already exist before creation
* **etl.py**: Performs ETL on data

### Run create_tables.py

`python create_tables.py`


### Run etl.py

`python etl.py`


## Summary

Project 2: Data Modelling with PostgreSQL provides startup Sparkify tools to analyse their data and helps them answer key business questions like _"What artist and song was listened during certain session?"_, _"What was the artist, song and user during certain session?"_, or _"What users have listened certain song?"_.
