
all_violations_table_create = """
CREATE TABLE IF NOT EXISTS MOCO_traffic_violations
(
	seqid VARCHAR(256)   
	,date_of_stop VARCHAR(256)  
	,time_of_stop VARCHAR(256)   
	,agency VARCHAR(256)   
	,subagency VARCHAR(256)   
	,description VARCHAR(256)   
	,location VARCHAR(256)   
	,latitude DOUBLE PRECISION   
	,longitude DOUBLE PRECISION  
	,accident VARCHAR(256)   
	,belts VARCHAR(256)   
	,personal_injury VARCHAR(256)   
	,property_damage VARCHAR(256)   
	,fatal VARCHAR(256)   
	,commercial_license VARCHAR(256)   
	,hazmat VARCHAR(256)   
	,commercial_vehicle VARCHAR(256)   
	,alcohol VARCHAR(256)   
	,work_zone VARCHAR(256)   
	,search_conducted VARCHAR(256)   
	,search_disposition VARCHAR(256)  
	,search_outcome VARCHAR(256)   
	,search_reason VARCHAR(256)  
	,search_reason_for_stop VARCHAR(256)   
	,search_type VARCHAR(256)   
	,search_arrest_reason VARCHAR(256)  
	,state VARCHAR(256)  
	,vehicletype VARCHAR(256)  
	,year BIGINT   
	,make VARCHAR(256)  
	,model VARCHAR(256)   
	,color VARCHAR(256)  
	,violation_type VARCHAR(256)  
	,charge VARCHAR(256) 
	,article VARCHAR(256)  
	,contributed_to_accident BOOLEAN  
	,race VARCHAR(256)   
	,gender VARCHAR(256)   
	,driver_city VARCHAR(256)  
	,driver_state VARCHAR(256)  
	,dl_state VARCHAR(256) 
	,arrest_type VARCHAR(256) 
	,geolocation VARCHAR(256)  
	,ZIP5 VARCHAR(5)
)
"""
all_violations_table_drop = """
    drop table if exists MOCO_traffic_violations
"""

staging_violations_table_create = """
CREATE TABLE IF NOT EXISTS staging_violations
(
	seq_id                  VARCHAR(255)
    ,date_of_stop                VARCHAR(255)
    ,time_of_stop                VARCHAR(255)
    ,agency                      VARCHAR(255)
    ,subagency                   VARCHAR(255)
    ,description                 VARCHAR(255)
    ,location                    VARCHAR(255)
    ,latitude                    DOUBLE PRECISION  
    ,longitude                   DOUBLE PRECISION  
    ,accident                    VARCHAR(255)
    ,belts                       VARCHAR(255)
    ,personal_injury             VARCHAR(255)
    ,property_damage             VARCHAR(255)
    ,fatal                       VARCHAR(255)
    ,commercial_license          VARCHAR(255)
    ,hazmat                      VARCHAR(255)
    ,commercial_vehicle          VARCHAR(255)
    ,alcohol                     VARCHAR(255)
    ,work_zone                   VARCHAR(255)
    ,state                       VARCHAR(255)
    ,vehicle_type                VARCHAR(255)
    ,year                        BIGINT
    ,make                        VARCHAR(255)
    ,model                       VARCHAR(255)
    ,color                       VARCHAR(255)
    ,violation_type              VARCHAR(255)
    ,charge                      VARCHAR(255)
    ,contributed_to_accident     BOOLEAN
    ,race                        VARCHAR(255)
    ,gender                      VARCHAR(255)
    ,driver_city                 VARCHAR(255)
    ,driver_state                VARCHAR(255)
    ,dl_state                    VARCHAR(255)
    ,arrest_type                 VARCHAR(255)
    ,geolocation                 VARCHAR(255)
    ,search_conducted            VARCHAR(255)
    ,search_outcome              VARCHAR(255)
    ,search_reason_for_stop      VARCHAR(255)
    ,article                     VARCHAR(255)
    ,ZIP5                        VARCHAR(5)
)
"""
staging_violations_table_drop = """
    drop table if exists staging_violations
"""

staging_violations_table_delete = """
    delete from moco_traffic_violations
    where {}
"""

staging_violations_table_insert = """
    insert into moco_traffic_violations (
        seqid
        ,date_of_stop
        ,time_of_stop
        ,agency
        ,subagency
        ,description
        ,location
        ,latitude
        ,longitude
        ,accident
        ,belts
        ,personal_injury
        ,property_damage
        ,fatal
        ,commercial_license
        ,hazmat
        ,commercial_vehicle
        ,alcohol
        ,work_zone
        ,search_conducted
        ,search_disposition
        ,search_outcome
        ,search_reason
        ,search_reason_for_stop
        ,search_type
        ,search_arrest_reason
        ,state
        ,vehicletype
        ,year
        ,make
        ,model
        ,color
        ,violation_type
        ,charge
        ,article
        ,contributed_to_accident
        ,race
        ,gender
        ,driver_city
        ,driver_state
        ,dl_state
        ,arrest_type
        ,geolocation
        ,zip5
    )
    select seq_id as seqid 
         , to_date(date_of_stop, 'MM/DD/YYYY') as date_of_stop 
         ,time_of_stop
         ,agency
         ,subagency
         ,description
         ,location
         ,latitude
         ,longitude
         ,accident
         ,belts
         ,personal_injury
         ,property_damage
         ,fatal
         ,commercial_license
         ,hazmat
         ,commercial_vehicle
         ,alcohol
         ,work_zone
         ,search_conducted
         ,NULl as search_disposition
         ,search_outcome
         ,NULL as search_reason 
         ,search_reason_for_stop
         ,NULL as search_type 
         ,NULL as search_arrest_reason
         ,state
         ,vehicle_type as vehicletype
         ,year
         ,make
         ,model
         ,color
         ,violation_type
         ,charge
         ,article
         ,contributed_to_accident
         ,race
         ,gender
         ,driver_city
         ,driver_state
         ,dl_state
         ,arrest_type
         ,geolocation
         ,zip5
    from staging_violations
"""

census_tract_drop = "drop table if exists census_tract"
census_tract_create = """
    CREATE TABLE IF NOT EXISTS census_tract (
         TractId    	varchar(255)
        ,State	        varchar(255)
        ,County	        varchar(255)
        ,TotalPop	    DOUBLE PRECISION
        ,Men	        DOUBLE PRECISION
        ,Women	        DOUBLE PRECISION
        ,Hispanic	    DOUBLE PRECISION
        ,White	        DOUBLE PRECISION
        ,Black	        DOUBLE PRECISION
        ,Native	        DOUBLE PRECISION
        ,Asian	        DOUBLE PRECISION
        ,Pacific        DOUBLE PRECISION
        ,Income	        DOUBLE PRECISION
        ,Poverty	    DOUBLE PRECISION
        ,Unemployment   DOUBLE PRECISION
    )
"""

zip_tract_drop = "drop table if exists zip_tract"
zip_tract_create = """
    CREATE TABLE IF NOT EXISTS zip_tract (
          ZIP       varchar(5)
         ,Tract    	varchar(255)
    )
"""