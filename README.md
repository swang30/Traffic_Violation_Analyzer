# Montgomery County Traffic Violation Analyzer

### Overview
This project implements an ETL framework to process traffic violation data from Montgomery County, MD, as well as demographic meta data. The framework extracts data from an API call, transforms the data, uploads to an S3 bucket and loads into Redshift warehouses,. The goal is to stage data in AWS Redshift and to understand police stops in relation to demographic attributes, including race, gender, zipcode, population density, poverty rate.

### Data Sources
* **[Montgomery County Open Data Portal](https://data.montgomerycountymd.gov/Public-Safety/Traffic-Violations/4mse-ku6q)**: Traffic violation data is extracted from the public data portal through sodapy API.
* **[ZIPCODE-TRACT CROSSWALK](https://www.huduser.gov/portal/datasets/usps_crosswalk.html)** A lookup table that contains ZIPCODE to Census-tract
* **[US Census Demographic Data](https://www.kaggle.com/muonneutrino/us-census-demographic-data)** Demographic data on census tract granularity

### Files
* **credential.cfg**: Config file that stores AWS credentials and S3 path
* **etl.py**: Load and insert tables
* **sql_queries**: sql statements to drop/create tables and copy/insert tables

### Data Model
![alt text](ERD.png "Logo Title Text 1")

### Data Model Use Case
1. To understand police stops in relation to demographic attributes, including race, gender, zipcode, population density, poverty rate.
~~~~sql
select a.ZIP5, count(*), sum(c.TotalPop), avg(c.Income), avg(c.Poverty), avg(c.Unemployment)
from moco_traffic_violations a
left join zip_tract b
    on a.ZIP5 = b.ZIP
left join census_tract c
    on b.Tract = b.TractId
group by a.ZIP5
order by 1
~~~~
2. To understand traffic violation trends
~~~~sql
select date_of_stop, count(*)
from moco_traffic_violations
group by date_of_stop
order by 1
~~~~
3. Who will use the data
    * Montgomery County residents
    * People who are interested in police activities
    
### ETL Process
1. Load data from API that looks back two days of data. This is limit the file size for faster processing
2. Transform data to have appropriate data types and append ZIP5.
3. Upload the data to s3
4. Load the data to a staging table in Redshift and insert into the warehouse.

### Steps Taken in Project
1. Step 1
    * Select police and demographic data as source.
    * The traffic violation data has over 1.5MM rows.
    * The data pipeline uses both API and csv as data source/format.
    * The end use case is to build analytical table and dashboard to visualize police stop trends and its relation to demographics.
2. Step 2
    * The traffic violation data has geographic coordinates but is missing zipcode that can be used to join to ohter demographic data.
    * The data pipeline maps the coordinates to zipcode
3. Step 3
    * Data Model: see the above ERD
4. Step 4
    * Data Dictionary: data_dictionary.xlsx
    * Data Quality Check
        * Checks if data fetched from API are not empty
        * Checks if expected data are inserted into the redshift table
5. Step 5
    * Choose AWS Redshift as the data warehousing tool because it's easy to access and query.
    * Choose Pandas as the Python ETL tool because the data is within reasonable size that my local computer has enough memory to read.
    * Steps to run
        * Configure a redshift cluster
        * Register an account in https://data.montgomerycountymd.gov/Public-Safety/Traffic-Violations/4mse-ku6q
        * Fill in credentials in credential.cfg
        * If first time running the program
            * Update SETUP flag to True in etl.py
        * Else
            * Run etl.py
    * Data Cadence: Every day. Since the backend data is refreshed daily
    * Scenarios
        * If the data was increased by 100x.
            * If read > write
                * Should still use AWS Redshift as the tool for data warehouse but scale its capacity by adding more nodes and space to handle more read requests.
            * If write > read
                * Switch the ETL tool to pyspark and consider hosting data in s3 with partitioned parquet. The reason is parquet is optimized with storeage.
        * If the pipelines were run on a daily basis by 7am.
            * Utilize airflow as the automation tool. In case dag fails, email triggers can be configured to send alerts and dashboard will be suspended until data is refreshed.
        * If the database needed to be accessed by 100+ people.
            * Since Redshift can handle 500 concurrent connections, the data will still stay on redshift. However, limit should be set for allowable concurrency. To be more cost-effective, the data can be migrated to an S3 bucket that should lower the storage cost. It's unliekly my project will be access by 100+ connections at the same time in that the data is specific to a county and users are mostly limited to the county residents.

