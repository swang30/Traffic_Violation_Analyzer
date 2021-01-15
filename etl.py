import sys
sys.path.append('C:\\Users\sswan\iCloudDrive\_CODE\\Udacity_Data_Engineer\Capstone_Proj')

import pandas as pd
from sodapy import Socrata
import configparser
from uszipcode import SearchEngine
from datetime import datetime, timedelta
import s3fs
import psycopg2
from Capstone_Proj.sql_queries import *

def load_data_main(where, limit=10000):
    """
    load the traffic violation from API call
    :param where: date filter
    :param limit: max rows fetched
    :return: Pandas Dataframe
    """
    client = Socrata("data.montgomerycountymd.gov", app_token=config['API']['APP_TOKEN'])
    results = client.get("4mse-ku6q", limit=limit, where=where)
    return pd.DataFrame.from_records(results)
    # results_df.to_csv('C:\\Users\sswan\iCloudDrive\_CODE\\Udacity_Data_Engineer\Capstone_Proj\Data\MOCO_Traffic_Violation_Sample.csv')

def load_data_meta(file_path, file_type='csv'):
    """
    Load meta data
    :param file_path: file path
    :param file_type: file type
    :return: Pandas Dataframe
    """
    if file_type == 'csv':
        return pd.read_csv(file_path)
    elif file_type == 'json':
        return pd.read_json(file_name)
    else:
        print('Unsupported File Type: {}'.format(file_type))

def geo_to_zip(lat, lon, search_engine):
    """
    Converts a coordinate to zipcode
    :param lat: Latitude
    :param lon: Longitude
    :param search_engine: search engine initialized by uszipcode
    :return: Pandas zipcode

    ------------------------------------------------------------------------------------------------------------------
    NOTE
    Modification to search.py: line 366 - 372. Enforce _n_radius_param_not_null = 3, otherwise it won't work with numpy arrays
    _n_radius_param_not_null = sum([
            isinstance(39.118589, (int, float)),
            isinstance(-77.168244, (int, float)),
            isinstance(25, (int, float)),
        ])
    ------------------------------------------------------------------------------------------------------------------
    Other package
        https://gis.stackexchange.com/questions/352961/convert-lat-lon-to-zip-postal-code-using-python
        geolocator = geopy.Nominatim(user_agent='swang')
        location = geolocator.reverse((39.181745, -77.2606))
        location.raw['address']['postcode']
    """
    ret = search_engine.by_coordinates(lat, lon, returns=1)
    if len(ret) > 0:
        return ret[0].zipcode
    else:
        return None

def transform_data_main(df_main, search_engine,lat='latitude', lon='longitude'):
    """
    transform df_main
    :param df_main: input dataframe
    :param search_engine: search engine initialized by uszipcode
    :param lat: Latitude
    :param lon: Longitude
    :return: None
    """
    # dtype transformation
    df_main[lat] = df_main[lat].astype(float)
    df_main[lon] = df_main[lon].astype(float)

    # df_main['ZIP5'] = df_main.apply(lambda x: geo_to_zip(x[lat], x[lat], search_engine=search_engine), axis=1)
    df_main.apply(lambda x: print(x[lat], x[lat]), axis=1)
    return df_main.apply(lambda x: geo_to_zip(x[lat], x[lat], search_engine=search_engine), axis=1)
    # np.vectorize(geo_to_zip)(results_df["latitude"].values, results_df["longitude"].values)

def df_to_s3(df : pd.DataFrame, s3_path, file_name):
    """
    Dataframe to S3
    :param df: input dataframe
    :param s3_path: s3 path
    :param file_name: s3 file name
    :return: None
    """
    bytes_to_write = df.to_csv(None, header=False, index=False).encode()
    fs = s3fs.S3FileSystem(key=config['AWS']['KEY'], secret=config['AWS']['SECRET'])
    with fs.open('s3://{}/{}'.format(s3_path, file_name), 'wb') as f:
        f.write(bytes_to_write)

def s3_to_redshift(cur, conn, copy_cmd, create_table_query, drop_table_query=""):
    """
    s3 to redshift
    :param cur: cursor
    :param conn: connection
    :param copy_cmd: copy command
    :param create_table_query: create table query
    :param drop_table_query: if empty, do not drop the table
    :return: None
    """
    if drop_table_query != "":
        cur.execute(drop_table_query)
        conn.commit()
    cur.execute(create_table_query)
    conn.commit()
    cur.execute(copy_cmd)
    conn.commit()

if __name__ == "__main__":
    # Use SETUP flag to create the main table in redshift and backfill historical data
    SETUP = False

    config = configparser.ConfigParser()
    config_path = 'C:\\Users\sswan\iCloudDrive\_CODE\\Udacity_Data_Engineer\Capstone_Proj\credential.cfg'
    config.read(config_path)

    s3_path = "udacity-data-engineer-sw/Capstone_Project"
    today = datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.now() - timedelta(days = 1)).strftime('%Y-%m-%d')
    where_clause = "date_of_stop in('{}','{}')".format(today,yesterday)
    search = SearchEngine(simple_zipcode=True)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    if SETUP:
        # Load historical file
        df_all = pd.read_csv('Capstone_Proj/Data/Traffic_Violations_20201101.csv')
        df_all['ZIP5'] = df_all.apply(lambda x: geo_to_zip(x['Latitude'], x['Longitude'], search_engine=search), axis=1)
        file_name = "Traffic_Violations_ALL.csv"
        df_to_s3(df_all, s3_path=s3_path, file_name=file_name)

        all_violations_copy = ("""
                copy MOCO_traffic_violations from 's3://{}/{}'
                credentials 'aws_iam_role={}'
                COMPUPDATE OFF region 'us-east-2'
                delimiter ','
                CSV QUOTE '\"'
                IGNOREHEADER 1
            """).format(s3_path, file_name, config['IAM_ROLE']['ARN'])
        s3_to_redshift(cur, conn, all_violations_copy, all_violations_table_create,all_violations_table_drop)

        del df_all

        #################### Load meta data ####################
        # census tract
        df_census_tract = load_data_meta('Capstone_Proj/Data/acs2017_census_tract_data.csv')
        df_census_tract = df_census_tract[['TractId','State','County','TotalPop','Men','Women','Hispanic','White','Black','Native','Asian','Pacific','Income','Poverty','Unemployment']]
        file_name = "acs2017_census_tract_data_thin.csv"
        df_to_s3(df_census_tract, s3_path=s3_path, file_name=file_name)

        census_tract_copy = ("""
                        copy census_tract from 's3://{}/{}'
                        credentials 'aws_iam_role={}'
                        COMPUPDATE OFF region 'us-east-2'
                        delimiter ','
                        CSV QUOTE '\"'
                    """).format(s3_path, file_name, config['IAM_ROLE']['ARN'])
        s3_to_redshift(cur, conn, census_tract_copy, census_tract_create,census_tract_drop)

        # zip tract
        df_zip_tract = load_data_meta('Capstone_Proj/Data/ZIP_TRACT_092020.xlsx', dtype={'ZIP': str, 'TRACT': str})
        df_zip_tract = df_zip_tract[['ZIP','TRACT']]
        file_name = "ZIP_TRACT_092020_thin.csv"
        df_to_s3(df_zip_tract, s3_path=s3_path, file_name=file_name)

        zip_tract_copy = ("""
                                copy zip_tract from 's3://{}/{}'
                                credentials 'aws_iam_role={}'
                                COMPUPDATE OFF region 'us-east-2'
                                delimiter ','
                                CSV QUOTE '\"'
                            """).format(s3_path, file_name, config['IAM_ROLE']['ARN'])
        s3_to_redshift(cur, conn, zip_tract_copy, zip_tract_create, zip_tract_drop)


    # Extract data
    df_staging = load_data_main(where=where_clause)

    ## Data Quality Check
    if len(df_staging) == 0:
        raise ValueError('API returned 0 record. Check where clause {}'.format(where_clause))

    # Transform data
    df_staging['latitude'] = df_staging['latitude'].astype(float)
    df_staging['longitude'] = df_staging['longitude'].astype(float)
    df_staging['ZIP5'] = df_staging.apply(lambda x: geo_to_zip(x['latitude'], x['longitude'], search_engine=search),axis=1)

    # Load data to S3
    file_name = "Traffic_Violations_{}.csv".format(today.replace("-",""))
    df_to_s3(df_staging,s3_path=s3_path, file_name=file_name)

    # Load data from S3 to Redshift
    staging_violations_copy = ("""
        copy staging_violations from 's3://{}/{}'
        credentials 'aws_iam_role={}'
        COMPUPDATE OFF region 'us-east-2'
        delimiter ','
        CSV QUOTE '\"'
    """).format(s3_path, file_name, config['IAM_ROLE']['ARN'])
    s3_to_redshift(cur, conn, staging_violations_copy, staging_violations_table_create,staging_violations_table_drop)

    # Delete and insert into main table
    staging_violations_table_delete = staging_violations_table_delete.format(where_clause)
    cur.execute(staging_violations_table_delete)
    conn.commit()
    cur.execute(staging_violations_table_insert)
    conn.commit()

    ## Data Quality Check
    sql_QC = """
        select count(*) 
        from moco_traffic_violations
        where {}
    """.format(where_clause)
    cur.execute(sql_QC)
    result = cur.fetchall()
    if result[0][0] == 0:
        raise ValueError('Insert Failed! Check moco_traffic_violations')