# Import necessary libraries and modules
import os
import requests
import pandas as pd
import datetime
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Create a SparkSession
spark = SparkSession.builder.appName("AirQualityConversion").getOrCreate()

# Function to process station data fetched from API
def process_station_data(data):
    stationid = data['stationID']
    name_eng = data['nameEN']
    loc_lat = data['lat']
    loc_long = data['long']
    area_eng = data['areaEN']
    parts = area_eng.split(', ')
    # Split area_eng to extract station and province information
    if len(parts) >= 3:
        station = parts[0] + ', ' + parts[1]
        province = parts[2].rstrip(' ')
    else:
        province = None 

    prov_id = None  
    # Lookup province_id using Spark DataFrame
    if province:
        try:
            prov_id = prov_sql.filter(prov_sql['province_name'] == province).collect()[0]['province_id']
        except IndexError:
            pass  

    date_ori = data['AQILast']['date']
    aqi = data['AQILast']['PM25']['aqi']

    return stationid, name_eng, loc_lat, loc_long, prov_id, date_ori, aqi, area_eng

# Function to read data from Google Sheets into a Pandas DataFrame
def read_google_sheets(service, spreadsheet_id, range_name):
    result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id,range=range_name).execute()
    values = result.get('values', [])
    return pd.DataFrame(values[1:], columns=values[0])

# Function to cast columns to specified data type in PySpark DataFrame
def cast_columns_to_type(df, columns, data_type):
    for c in columns:
        try:
            df = df.withColumn(c, col(c).cast(data_type))
        except:
            continue
    return df

# Function to fetch station data from API
def fetch_station_data(base_url, station_id, headers):
    url = base_url + station_id.lower()
    response = requests.get(url, headers=headers)
    return response.json()

# Function to find region information for station data
def find_region_info(station_data):
    station_id = station_data['stationID']
    area_eng = station_data['areaEN']
    lat_add = station_data['lat']
    long_add = station_data['long']
    parts = area_eng.split(', ')

    if len(parts) >= 3:
        station = parts[0] + ', ' + parts[1] 
        province = parts[2].rstrip(' ')
    else:
        return None

    province = province.replace(" Province","").replace("Chang Wat ","").replace("Samut Prakan\xa0","Samut Prakan").replace("Chon Buri","Chon buri")

    # Perform lookup using Spark DataFrame
    lookup_df = prov_sql.filter(prov_sql.province_name == province).select("province_id")
    lookup_result = lookup_df.collect()

    if lookup_result:
        province_id = lookup_result[0].province_id
        return (station_id, station, area_eng, lat_add, long_add, province_id)
    else:
        print("New province name : " + province)
        return None
    
# Configuration and setup

# Base URL for fetching station data
base_url = "http://air4thai.com/forweb/getAQI_JSON.php?stationID="
headers = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"
}

# Spark SQL DataFrame for province information
prov_sql = spark.sql("SELECT province_name, province_id FROM default.provinces")

# Fetch data for all stations
data = {}
station_ids = ['02t','03t','05t','08t','11t','12t','13t','14t','16t','17t','18t','19t','20t','21t','22t','24t','25t',
            '26t','27t','28t','29t','31t','33t','34t','35t','36t','37t','38t','39t','40t','41t','42t','43t',
            '44t','46t','47t','50t','52t','53t','54t','57t','58t','59t','60t','61t','62t','63t','67t','68t','69t','70t',
            '71t','72t','73t','74t','75t','76t','77t','78t','79t','80t','81t']

for station_id in station_ids:
    try:
        station_data = fetch_station_data(base_url, station_id, headers)
        data[station_id] = station_data
    except Exception as e:
        print(f"Error fetching data for station {station_id}: {e}")

# Collect all results outside the loop
station_results = []
for station_id in data:
    result = find_region_info(data[station_id])
    if result:
        station_results.append(result)

# Create DataFrame once outside the loop
df_stations = spark.createDataFrame(station_results, schema=["station_code", "station_name", "station_address", "lat", "long", "province_id"])

# Fetch Google Sheets data and convert to PySpark DataFrames
data_processed = []
creds = None

# Check if token file exists and credentials are valid
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SAMPLE_SPREADSHEET_ID = "19aFrlAgtPn6Nuv1CjUIqTUhDvZSRnZU8Z_hi8jGBg2c"

# columns to chang type
long_cols = ['year', 'province_id', 'burned_area','factory_type_id','region_id','Level','Min','Max']
str_cols = ['factory_type_name','province_factory_area','province_name','region_name','station_code','station_name','station_address']
date_cols = ['date','string','date']
double_cols = ['value','lat','long']

for station_id in data:
    processed_data = process_station_data(data[station_id])
    data_processed.append(processed_data)

raw_columns = ["station_code", "station_name", "lat", "long", "province_id","date","value","station_address"]
raw_data = spark.createDataFrame(data_processed, raw_columns)
pm_25 = raw_data.select("date","station_code" , "value")

# Fetch Google Sheets data and convert to PySpark DataFrames

# Check if token file exists and credentials are valid
if os.path.exists("/Workspace/Users/jirasaksaimek@gmail.com/token.json"):
    creds = Credentials.from_authorized_user_file("/Workspace/Users/jirasaksaimek@gmail.com/token.json", SCOPES)
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
        creds = flow.run_local_server(port=0)

    with open("/Workspace/Users/jirasaksaimek@gmail.com/token.json", "w") as token:
        token.write(creds.to_json())

# Build Google Sheets service
service = build("sheets", "v4", credentials=creds)

# Read data from Google Sheets into Pandas DataFrames
burned_areas_df = read_google_sheets(service, SAMPLE_SPREADSHEET_ID, "burned_areas!A:C")
region_df = read_google_sheets(service, SAMPLE_SPREADSHEET_ID, "region!A:B")
province_factories_df = read_google_sheets(service, SAMPLE_SPREADSHEET_ID, "province_factories!A:C")
factory_types_df = read_google_sheets(service, SAMPLE_SPREADSHEET_ID, "factory_types!A:B")
provinces_df = read_google_sheets(service, SAMPLE_SPREADSHEET_ID, "provinces!A:C")
levels_df = read_google_sheets(service, SAMPLE_SPREADSHEET_ID, "levels!A:D")

# Convert Pandas DataFrames to PySpark DataFrames
burned_areas_spark_df = spark.createDataFrame(burned_areas_df)
region_spark_df = spark.createDataFrame(region_df)
province_factories_spark_df = spark.createDataFrame(province_factories_df)
province_factories_spark_df = province_factories_spark_df.select("factory_type_id","province_id","province_factory_area")
factory_types_spark_df = spark.createDataFrame(factory_types_df)
provinces_spark_df = spark.createDataFrame(provinces_df)
levels_spark_df = spark.createDataFrame(levels_df)

# Chang columns type to long
burned_areas_spark_df = cast_columns_to_type(burned_areas_spark_df, long_cols, 'long')
region_spark_df = cast_columns_to_type(region_spark_df, long_cols, 'long')
province_factories_spark_df = cast_columns_to_type(province_factories_spark_df, long_cols, 'long')
factory_types_spark_df = cast_columns_to_type(factory_types_spark_df, long_cols, 'long')
provinces_spark_df = cast_columns_to_type(provinces_spark_df, long_cols, 'long')
levels_spark_df = cast_columns_to_type(levels_spark_df, long_cols, 'long')

# Chang columns type to date
burned_areas_spark_df = cast_columns_to_type(burned_areas_spark_df, date_cols, 'date')
region_spark_df = cast_columns_to_type(region_spark_df, date_cols, 'date')
province_factories_spark_df = cast_columns_to_type(province_factories_spark_df, date_cols, 'date')
factory_types_spark_df = cast_columns_to_type(factory_types_spark_df, date_cols, 'date')
provinces_spark_df = cast_columns_to_type(provinces_spark_df, date_cols, 'date')
pm_25 = cast_columns_to_type(pm_25, date_cols, 'date')
df_stations = cast_columns_to_type(df_stations, date_cols, 'long')

# Chang columns type to double
burned_areas_spark_df = cast_columns_to_type(burned_areas_spark_df, double_cols, 'double')
region_spark_df = cast_columns_to_type(region_spark_df, double_cols, 'double')
province_factories_spark_df = cast_columns_to_type(province_factories_spark_df, double_cols, 'double')
factory_types_spark_df = cast_columns_to_type(factory_types_spark_df, double_cols, 'double')
provinces_spark_df = cast_columns_to_type(provinces_spark_df, double_cols, 'double')
df_stations = cast_columns_to_type(df_stations, double_cols, 'double')

# Write data to Delta tables
# Append PM2.5 data to existing 'pm2_5' Delta table
pm_25.write.format("delta").mode("append").saveAsTable("default.pm2_5")

# Overwrite 'provincefactories' Delta table with new data and merge schema
burned_areas_spark_df.write.format("delta").mode("overwrite").saveAsTable("default.burnedareas")
province_factories_spark_df.write.format("delta").mode("overwrite").saveAsTable("default.provincefactories")

#create table first
try:
    region_spark_df.write.format("delta").saveAsTable("default.regions")
except:
    pass
try:
    factory_types_spark_df.write.format("delta").saveAsTable("default.factorytypes")
except:
    pass
try:
    provinces_spark_df.write.format("delta").saveAsTable("default.provinces")
except:
    pass
try:
    df_stations.write.format("delta").saveAsTable("default.stations")
except:
    pass
try:
    levels_spark_df.write.format("delta").saveAsTable("default.levels")
except:
    pass

# Upsert data into 'regions', 'factorytypes', 'provinces', and 'stations' Delta tables
# Upsert 'regions' table
region_spark_df.createOrReplaceTempView('region')
Update_region_province =  f"""
    MERGE INTO default.regions AS target
    USING region AS source
    ON source.region_id = target.region_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
spark.sql(Update_region_province)

# Upsert 'factorytypes' table
factory_types_spark_df.createOrReplaceTempView('factory_types')
Update_region_province =  f"""
    MERGE INTO default.factorytypes AS target
    USING factory_types AS source
    ON source.factory_type_id = target.factory_type_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
spark.sql(Update_region_province)

# Upsert 'provinces' table
provinces_spark_df.createOrReplaceTempView('provinces')
Update_region_province =  f"""
    MERGE INTO default.provinces AS target
    USING provinces AS source
    ON source.province_id = target.province_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
spark.sql(Update_region_province)

# Upsert 'stations' table
df_stations.createOrReplaceTempView('df_stations')
upsert_query = """
    MERGE INTO default.stations AS target
    USING df_stations AS source
    ON source.station_code = target.station_code
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
"""
spark.sql(upsert_query)

# Upsert 'levels' table
levels_spark_df.createOrReplaceTempView('levels')
Update_levels_province =  f"""
    MERGE INTO default.levels AS target
    USING levels AS source
    ON source.Level = target.Level
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
spark.sql(Update_region_province)

# code create view v_pm2_5bystation
create_view_v_pm2_5bystation = """Create view v_pm2_5bystation as
WITH  PM2_5ByStation AS ( SELECT station_code AS Station,
    ROUND(FLOOR(AVG(CAST(value AS DOUBLE))), 0) AS Avg,
    ROUND(MAX(CAST(value AS DOUBLE)), 0) AS Max,
    ROUND(MIN(CAST(value AS DOUBLE)), 0) AS Min
  FROM v_pm2_5
  GROUP BY station_code
), avg_levels AS (
  SELECT pm.Station, pm.Avg, lvl.Level AS AvgLevel
  FROM PM2_5ByStation pm
  LEFT JOIN default.levels lvl ON (pm.Avg >= lvl.Min) AND (pm.Avg <= lvl.Max)
), max_levels AS (
  SELECT pm.Station, pm.Max, lvl.Level AS MaxLevel
  FROM PM2_5ByStation pm
  LEFT JOIN default.levels lvl ON (pm.Max >= lvl.Min) AND (pm.Max <= lvl.Max)
), min_levels AS (
  SELECT pm.Station, pm.Min, lvl.Level AS MinLevel
  FROM PM2_5ByStation pm
  LEFT JOIN default.levels lvl ON (pm.Min >= lvl.Min) AND (pm.Min <= lvl.Max) 
), v_pm2_5bystation as (
  SELECT avg_levels.Station as station, avg_levels.AvgLevel as avgLevel, avg_levels.Avg as avg,
  max_levels.Max as max, min_levels.Min as min, max_levels.MaxLevel as maxlevel,  min_levels.MinLevel as minlevel
FROM avg_levels
JOIN max_levels ON avg_levels.Station = max_levels.Station
JOIN min_levels ON avg_levels.Station = min_levels.Station
)SELECT * FROM v_pm2_5bystation
"""

# code create view v_date
create_view_v_date = """Create view v_date as 
WITH date_range AS ( 
  SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM v_pm2_5 
  )
, date_range_expanded AS (
  SELECT EXPLODE(SEQUENCE(min_date, max_date, INTERVAL 1 DAY)) AS date 
  FROM date_range
)
, v_date AS (
  SELECT date,DATE_FORMAT(date, 'MMM') AS month_name,
    YEAR(date) AS year, MONTH(date) AS month_number
  FROM 
    date_range_expanded )
SELECT * FROM v_date 
"""

# code create view v_pm2_5
create_view_v_pm2_5 = """Create view v_pm2_5 as 
SELECT p.station_code, s.province_id, p.date, p.value, l.Level as level
FROM default.pm2_5 p
LEFT JOIN default.levels l ON p.value BETWEEN l.Min AND l.Max
LEFT JOIN default.stations s ON p.station_code = p.station_code
"""

# code create view v_burnedareas01
create_view_v_burnedareas = """Create view v_burnedareas01 as 
SELECT *, CONCAT(CASE 
                WHEN year > 2500 THEN year - 543
                ELSE year
            END, '-1-1') AS date_string
FROM default.burnedareas
"""

# create view in first
try:
    spark.sql(create_view_v_pm2_5bystation)
except:
    pass
try:
    spark.sql(create_view_v_date)
except:
    pass
try:
    spark.sql(create_view_v_pm2_5)
except:
    pass
try:
    spark.sql(create_view_v_burnedareas)
except:
    pass
