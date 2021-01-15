import pandas as pd
from uszipcode import SearchEngine
from Capstone_Proj.etl import geo_to_zip, s3_to_redshift

df_all = pd.read_csv('Capstone_Proj/Data/Traffic_Violations_20201101.csv')
search = SearchEngine(simple_zipcode=True)
df_all['ZIP5'] = df_all.apply(lambda x: geo_to_zip(x['Latitude'], x['Longitude'], search_engine=search), axis=1)
df_all.to_csv('Capstone_Proj/Data/Traffic_Violations_20201101_ZIP5.csv')

client = Socrata("data.montgomerycountymd.gov", app_token=config['API']['APP_TOKEN'])
results = client.get("4mse-ku6q", limit=10000, where="date_of_stop in ('2020-11-01','2020-10-30')")
df = pd.DataFrame.from_records(results)

df_test = pd.read_csv('C:\\Users\sswan\iCloudDrive\_CODE\\Udacity_Data_Engineer\Capstone_Proj\Data\Traffic_Violations_20201102.csv')

staging_violations_copy = ("""
    copy staging_violations from s3://{}/{}
    credentials 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-east-2'
""").format(s3_path, file_name, config['IAM_ROLE']['ARN'])
s3_to_redshift(cur, conn, staging_violations_copy)

df_all = pd.read_csv('Capstone_Proj/Data/Traffic_Violations_20201101_ZIP5.csv')


"""
    copy MOCO_traffic_violations from 's3://udacity-data-engineer-sw/Capstone_Project//Traffic_Violations_ALL.csv'
    credentials 'aws_iam_role=arn:aws:iam::238434588390:role/myRedshiftRole'
    COMPUPDATE OFF region 'us-east-2'      
"""

# Load meta data

# first time staging table setup
df_all = pd.read_csv('Capstone_Proj/Data/Traffic_Violations_20201101.csv',nrows=100)
df_all = df_all.head(1)

from sqlalchemy import create_engine
conn = create_engine('postgresql://awsuser:Wss960130@redshift-cluster-1.ch6lhm8l8g6h.us-east-2.redshift.amazonaws.com:5439/dev')
df_all.to_sql('df_test',conn,index=False, if_exists='replace')
# https://github.com/awslabs/amazon-redshift-utils/blob/master/src/AdminViews/v_generate_tbl_ddl.sql
# https://support.sqldbm.com/knowledge-bases/2/articles/2654-how-to-generate-sql-ddl-script-from-redshift-for-reverse-engineering

# import numpy as np
# from datetime import datetime
#
# print(datetime.now())
# np.vectorize(geo_to_zip)(df_all["Latitude"].values, df_all["Longitude"].values, search)
# print(datetime.now())
#
# print(datetime.now())
# df_all.apply(lambda x: geo_to_zip(x['Latitude'], x['Longitude'], search_engine=search), axis=1)
# print(datetime.now())
#
# import math
# math.cos(df_all["Latitude"].values) * 69.172
# df_all["Latitude"].apply(math.cos)
# geo_to_zip(df_all["Latitude"], df_all["Longitude"], search)