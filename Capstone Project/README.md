# ETL Pipeline - Immigration and Temperature Data
### Data Engineering Capstone Project

### To Run This Pipeline simply execute etl.py or Explore Using Capstone Notebook 

```
    python etl.py
```

#### Project Summary
The project's goal is to build an ETL Pipeline that usus I94 Immigration data and Temperature Data to create a Database optimized for read heavy workloads and OLAP queries. The database would contain information related to immigration events and temperature information. This can then be used to reveal immigration patterns, draw correlations and answer various questions relating to immigration and weather.


The project was written with the following workflow serving as a guideline:

* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up


### Step 1: Scope the Project and Gather Data

#### Scope 
In this project, I had to pull out the 194 Immigration city by destination to form our first dimension table, then for the second dimension table, I aggregated temperature data by city. Both tables were then joined using the destination city as the join key. This is to allow us answer questions like, "Does temperature affect the choice of destination cities for immigration.

SPARK was used for data processing because of its ability to handle different data formats, pull resources form different machines when run in a distributed manner and its familiar SQL-like interface along with its easy to use python API

PARQUET was used for data storage because of its columnar nature which makes it easy to query only selected rows when dealing with large datasets and its also readily available as a file format in the Hadoop Ecosystem


#### Describe and Gather Data 
The I94 immigration data is from [The US National Tourism and Trade Office Website](https://travel.trade.gov/research/reports/i94/historical/2016.html). It is provided in a binary database storage format which is easily handled by spark.

**Key Notes:**
- i94yr = Year
- i94mon = Month
- i94cit = Origin City Code
- i94port = Destination City Code
- arrdate = Arrival date in the US
- i94mode = Travel code
- depdate = Departure date from the US
- i94visa = Reason  

The temperature data set comes from [Open Data](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/). It is provided in csv format.
  
**Key Notes:**
- AverageTemperature = Average Temperature
- City = Name of city
- Country = Name of country
- Latitude= Latitude
- Longitude = Longitude



### Step 2: Explore and Assess the Data
#### Explore the Data 
**I94 immigration data** - 
For this immigration dataset we will remove all rows with invalid destination city codes, ie, XXX, double digit codes, NaNs, etc

**Temperature Data** - 
For the Temperature dataset we will remove all rows with invalid AverageTemperatures, ie, NaN and duplicates.
We will then proceed to enrich it with the destination city code


### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
**Facts Table** - This table contains information about the I94 Immigration data which has been enriched with the Temperature data

Columns:
- i94yr = Year
- i94mon = Month
- i94cit = Origin City Code
- i94port = Destination City Code
- arrdate = Arrival date in the US
- i94mode = Travel code
- depdate = Departure date from the US
- i94visa = Reason 
- AverageTemperature = average temperature of destination city

**1st Dimension Table** - This will contain events from the I94 immigration data.

Columns:
- i94yr = Year
- i94mon = Month
- i94cit = Origin City Code
- i94port = Destinantion City Code
- arrdate = Arrival date in the US
- i94mode = Travel code
- depdate = Departure Date from the US
- i94visa = Reason

**2nd Dimension Table** - This will contain city temperature data.

Columns:
- i94port = Mapped Destination City Code 
- AverageTemperature = Average Temperature
- City = City Name
- Country = Country Name
- Latitude= Latitude
- Longitude = Longitude


#### 3.2 Mapping Out Data Pipelines
Pipeline Steps:
1. Clean I94 dataset using steps outlined in step 2 and create Spark DataFrame for each month.
2. Clean Temperature Data using step outlined in step 2 to create Spark DataFrame.
3. Create Dimension Table from Immigration Dataset. This is done by selecting relevant columns from the immigration dataset and writing to disk using parquet as the file format and i94port as partition key
4. Create Dimension Table for Temperature Dataset: This is done by selecting relevant columns form the df_temp dataframe and writing to disk using parquet as a file format and i94port as partition key
5. Create Facts table by joining immigration and temperature dimension tables on i94port and write to parquet files using i94port as partition key 

### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
Build the data pipelines to create the data model.

#### 4.2 Data Quality Checks
Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:
 * Integrity constraints on the relational database (e.g., unique key, data type, etc.)
 * Unit tests for the scripts to ensure they are doing the right thing
 * Source/Count checks to ensure completeness
 

#### 4.3 Data dictionary 
**Facts Table** - This table contains information about the I94 Immigration data which has been enriched with the Temperature data

Columns:
- i94yr = Year
- i94mon = Month
- i94cit = Origin City Code
- i94port = Destination City Code
- arrdate = Arrival date in the US
- i94mode = Travel code
- depdate = Departure date from the US
- i94visa = Reason 
- AverageTemperature = average temperature of destination city

**1st Dimension Table** - This will contain events from the I94 immigration data.

Columns:
- i94yr = Year
- i94mon = Month
- i94cit = Origin City Code
- i94port = Destinantion City Code
- arrdate = Arrival date in the US
- i94mode = Travel code
- depdate = Departure Date from the US
- i94visa = Reason

**2nd Dimension Table** - This will contain city temperature data.

Columns:
- i94port = Mapped Destination City Code 
- AverageTemperature = Average Temperature
- City = City Name
- Country = Country Name
- Latitude= Latitude
- Longitude = Longitude


#### Step 5: Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project. <br>

In this project, I decided to usem Spark as the main Data Processing Engine, because of its ability to handle different data formats, pull resources form different machines when run in a distributed manner and its familiar SQL-like interface along with its easy to use python API. 

PARQUET was used for data storage because of its columnar nature which makes it easy to query only selected rows when dealing with large datasets and its also readily available as a file format in the Hadoop Ecosystem

Other Data Formats such as  SAS,CSV and TXT were also used in this project, because of the ease of use and manipulation

* Propose how often the data should be updated and why. <br>
The default format of the raw files is monthly, based on this, we should continue running the pipeline to update the data monthly.

### Scenarios
* Write a description of how you would approach the problem differently under the following scenarios:
    1. If the data was increased by 100x.
        - Load data into Amazon Redshift or Google Bigquery: It is an analytical database that is optimized for aggregation, read-heavy workloads and adhoc OLAP queries
    2. The data populates a dashboard that must be updated on a daily basis by 7am every day.
        - Using Apache Airflow as the orchestrator, create DAG retries and/or send emails on failures. Trigger DAGs Daily at 7am
        - Have daily quality checks. If checks fail, send emails to operators and disable dashboards
    3. The database needed to be accessed by 100+ people.
        - Use Redshift or Google Bigquery since it has auto-scaling capabilities and good read performance
