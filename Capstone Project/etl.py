#!/usr/bin/env python
# coding: utf-8

# # ETL Pipeline - Immigration and Temperature Data
# ### Data Engineering Capstone Project
# 
# #### Project Summary
# The project's goal is to build an ETL Pipeline that usus I94 Immigration data and Temperature Data to create a Database optimized for read heavy workloads and OLAP queries. The database would contain information related to immigration events and temperature information. This can then be used to reveal immigration patterns, draw correlations and answer various questions relating to immigration and weather.
# 
# 
# The project was written with the following workflow serving as a guideline:
# 
# * Step 1: Scope the Project and Gather Data
# * Step 2: Explore and Assess the Data
# * Step 3: Define the Data Model
# * Step 4: Run ETL to Model the Data
# * Step 5: Complete Project Write Up

import re
import logging
from datetime import datetime, timedelta
from pyspark.sql.functions import udf
import os
import pandas as pd
import psycopg2
from collections import defaultdict
from pyspark.sql import SparkSession


# LOAD THE IMMIGRATION DATA USING PANDAS INBUILT SAS READER, WHILE MAKING SURE TO PASS THE ENCODING AS A PARAMETER
logging.info('LOAD THE IMMIGRATION DATA USING PANDAS INBUILT SAS READER, WHILE MAKING SURE TO PASS THE ENCODING AS A PARAMETER')
fname = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
df_im = pd.read_sas(fname, 'sas7bdat', encoding="ISO-8859-1")


# LOAD THE TEMPERATURE DATA AS WITH READ_CSV
logging.info('LOAD THE TEMPERATURE DATA AS WITH READ_CSV')
fname = '../../data2/GlobalLandTemperaturesByCity.csv'
df_temp = pd.read_csv(fname, sep=',')

# CREATE SPARK SESSION USING SAS7BDAT JAR FILE 
logging.info('CREATE SPART SESSION USING SAS7BDAT JAR FILE ')
spark = SparkSession.builder.config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11").enableHiveSupport().getOrCreate()

# CREATE DICTIONARY OF VALID I94PORT CODES USING VALID_PORTS FILE
logging.info('CREATE DICTIONARY OF VALID I94PORT CODES USING VALID_PORTS FILE')
re_obj = re.compile(r'\'(.*)\'.*\'(.*)\'')
i94port_valid = {}
with open('valid_ports.txt') as f:
     for line in f:
         match = re_obj.search(line)
         i94port_valid[match[1]]=[match[2]]

# CLEAN I94 IMMIGRATION DATA
logging.info('CLEAN I94 IMMIGRATION DATA')
def clean_i94_data(file):
    """
    :params file: Input to path to immigration file
    :return SparkDataFrame: A Spark DataFrame containing immigration data with valid ports
    """    
    # LOAD IMMIGRATION DATA INTO SPARK
    logging.info('OAD IMMIGRATION DATA INTO SPARK')
    df_immigration = spark.read.format('com.github.saurfang.sas.spark').load(file)

    # REMOVE INVALID ENTRIES 
    logging.info('REMOVE INVALID ENTRIES')
    df_immigration = df_immigration.filter(df_immigration.i94port.isin(list(i94port_valid.keys())))

    return df_immigration


# PREVIEW IMMEGRATION FILES USING SPARK
logging.info('PREVIEW IMMEGRATION FILES USING SPARK')
immigration_test_file = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat' 
df_immigration_test = clean_i94_data(immigration_test_file)
df_immigration_test.show()

# LOAD TEMPERARTURE DATASET (CSV)
logging.info('LOAD TEMPERARTURE DATASET (CSV)')
df_temp = spark.read.format("csv").option("header", "true").load("../../data2/GlobalLandTemperaturesByCity.csv")

# REMOVE INVALID DATA POINTS
logging.info('REMOVE INVALID DATA POINTS')
df_temp = df_temp.filter(df_temp.AverageTemperature != 'NaN')

# REMOVE DUPPLICATE ENTRIES 
logging.info('REMOVE DUPPLICATE ENTRIES ')
df_temp = df_temp.dropDuplicates(['City', 'Country'])

# UDF TO MATCH I94PORT TO CORRECT PORT NAME IN IMMIGRATION DATASET
logging.info('DF TO MATCH I94PORT TO CORRECT PORT NAME IN IMMIGRATION DATASET')
@udf()
def get_i94port(city):
    """
    :params city: Name of City
    :return str: Correct i94 Port
    """
    for key in i94port_valid:
        if city.lower() in i94port_valid[key][0].lower():
            return key

# APPLY UDF TO MATCH PORT NAMES
logging.info('APPLY UDF TO MATCH PORT NAMES')
df_temp = df_temp.withColumn("i94port", get_i94port(df_temp.City))

# REMOVE ROWS WITHOUT I94PORT
logging.info('REMOVE ROWS WITHOUT I94PORT')
df_temp = df_temp.filter(df_temp.i94port != 'null')


# CREATE PATH TO STORE ETL RESULTS
logging.info('CREATE PATH TO STORE ETL RESULTS')
etl_results = 'etl_results'
try:
    os.mkdir(etl_results)
except FileExistsError:
    pass

# PATH TO IMMIGRATION DATA FILE 
logging.info('PATH TO IMMIGRATION DATA FILE')
immigration_data = '/data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'

# LOAD IMMIGRATION DATA INTO SPART DATAFRAME AND CLEAN 
logging.info('LOAD IMMIGRATION DATA INTO SPART DATAFRAME AND CLEAN')
df_immigration = clean_i94_data(immigration_data)

# SELECT RELEVANT COLUMNS TO BUILD IMMIGRATION DIMENSION TABLE
logging.info('SELECT RELEVANT COLUMNS TO BUILD IMMIGRATION DIMENSION TABLE')
immigration_table = df_immigration.select(["i94yr", "i94mon", "i94cit", "i94port", "arrdate", "i94mode", "depdate", "i94visa"])

# WRTIE IMMIGRATION DIMENSION TABLE TO DISK USING PARQUET FILE FORMAT AND I94PORT AS PARTITION KEY
logging.info('WRTIE IMMIGRATION DIMENSION TABLE TO DISK USING PARQUET FILE FORMAT AND I94PORT AS PARTITION KEY')
immigration_table.write.mode("append").partitionBy("i94port").parquet("{}/immigration.parquet".format(etl_results))

# SELECT RELEVANT COLUMNS FOR TEMPERATURE DIMENSION TABLE
logging.info('SELECT RELEVANT COLUMNS FOR TEMPERATURE DIMENSION TABLE')
temp_table = df_temp.select(["AverageTemperature", "City", "Country", "Latitude", "Longitude", "i94port"])

# WRTIE TEMPERATURE DIMENSION TABLE TO DISK USING PARQUET FILE FORMAT AND I94PORT AS PARTITION KEY
logging.info('WRTIE TEMPERATURE DIMENSION TABLE TO DISK USING PARQUET FILE FORMAT AND I94PORT AS PARTITION KEY')
temp_table.write.mode("append").partitionBy("i94port").parquet("{}/temperature.parquet".format(etl_results))

# CREATE TEMPORARY VIEWS OF THE IMMIGRATION AND TEMPERATURE DIMENSION TABLES
logging.info('CREATE TEMPORARY VIEWS OF THE IMMIGRATION AND TEMPERATURE DIMENSION TABLES')
df_immigration.createOrReplaceTempView("immigration_view")
df_temp.createOrReplaceTempView("temp_view")

# CREATE FACT TABLES BY JOINING IMMIGRATION AND TEMPERATURE VIEWS ON I94PORT
logging.info('CREATE FACT TABLES BY JOINING IMMIGRATION AND TEMPERATURE VIEWS ON I94PORT')
fact_table = spark.sql('''
SELECT immigration_view.i94yr as year,
       immigration_view.i94mon as month,
       immigration_view.i94cit as city,
       immigration_view.i94port as i94port,
       immigration_view.arrdate as arrival_date,
       immigration_view.depdate as departure_date,
       immigration_view.i94visa as reason,
       temp_view.AverageTemperature as temperature,
       temp_view.Latitude as latitude,
       temp_view.Longitude as longitude
FROM immigration_view
JOIN temp_view ON (immigration_view.i94port = temp_view.i94port)
''')

# WRTIE IMMIGRATION DIMENSION TABLE TO DISK USING PARQUET FILE FORMAT AND I94PORT AS PARTITION KEY
logging.info('WRTIE IMMIGRATION DIMENSION TABLE TO DISK USING PARQUET FILE FORMAT AND I94PORT AS PARTITION KEY')
fact_table.write.mode("append").partitionBy("i94port").parquet("{}/fact.parquet".format(etl_results))

def check_quality(df, description):
    """
    :params df: Spark DataFrame
    :params description: str, description of spark dataframe used as a discriminator
    :return Bool: Result of Data Quality Check
    """
    result = df.count()
    if result == 0:
        print("Data quality check failed for {} with zero records".format(description))
        return False
    else:
        print("Data quality check passed for {} with {} records".format(description, result))
        return True

# PERFORM DATA QUALITY CHECK
logging.info('PERFORM DATA QUALITY CHECK')
check_quality(df_immigration, "immigration table")
check_quality(df_temp, "temperature table")

