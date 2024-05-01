# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, struct, to_date

from pyspark.sql.types import IntegerType, StringType, StructField, StructType
import requests
from datetime import datetime
import pyspark.sql.functions as F
import datetime

# COMMAND ----------

# Create a SparkSession
spark = SparkSession.builder.appName("AirQualityConversion").getOrCreate()

# COMMAND ----------

import requests

def fetch_station_data(base_url, station_id, headers):

    url = base_url + station_id.lower()
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Request failed with status code {response.status_code}: {response.text}")

# Configuration
base_url = "http://air4thai.com/forweb/getAQI_JSON.php?stationID="
headers = {
  "Accept": "application/json",
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"
}

# Loop through station IDs 01-100
results = {}  # Store fetched results by station ID 
for station_num in range(1, 101):
    station_id = f"{station_num:02d}t"  # Format as '01t', '02t', etc.

    try:
        data = fetch_station_data(base_url, station_id, headers)
        results[station_id] = data
    except Exception as e:
        print(f"Error fetching data for station {station_id}: {e}")

# COMMAND ----------

region_data = {
    "region_d1": {
        "region_id": "1",
        "region_name": "North",
        "provinces_id_n": {
            "Chiang Mai": "38",
            "Lamphun": "39",
            "Lampang": "40",
            "Uttaradit": "41",
            "Phrae": "42",
            "Nan": "43",
            "Phayao": "44",
            "Chiang Rai": "45",
            "Mae Hong Son": "46"
        }
    },
    "region_d2": {
        "region_id": "2",
        "region_name": "Centrl",
        "provinces_id_n": {
            "Bangkok": "1", 
            "Samut Prakan": "2", 
            "Nonthaburi": "3", 
            "Pathum Thani": "4", 
            "Phra Nakhon Si Ayutthaya": "5", 
            "Ang Thong": "6", 
            "Lopburi": "7", 
            "Sing Buri": "8", 
            "Chai Nat": "9", 
            "Saraburi": "10", 
            "Nakhon Nayok": "17", 
            "Nakhon Sawan": "47", 
            "Uthai Thani": "78", 
            "Kamphaeng Phet": "49", 
            "Sukhothai": "51", 
            "Phitsanulok": "52", 
            "Phichit": "53", 
            "Phetchabun": "54", 
            "Suphun Buri": "57", 
            "Nakhon Pathom": "58", 
            "Samut Sakhon": "59", 
            "Samut Songkhram": "60"
        }
    },
    "region_d3": {
        "region_id": "3",
        "region_name": "Northeast",
        "provinces_id_n": {
            "Nakhon Ratchasima": "19", 
            "Buri Ram": "20", 
            "Surin": "21", 
            "Si Sa Ket": "22", 
            "Ubon Ratchathani": "23", 
            "Yasothon": "24", 
            "Chaiyaphum": "25", 
            "Amnat Charoen": "26", 
            "Nong Bua Lam Phu": "27", 
            "Khon Kaen": "28", 
            "Udon Thani": "29", 
            "Loei": "30", 
            "Nong Khai": "31", 
            "Maha Sarakham": "32", 
            "Roi Et": "33", 
            "Kalasin": "34", 
            "Sakon Nakhon": "35", 
            "Nakhon Phanom": "36", 
            "Mukdahan": "37"
        }
    },
    "region_d4": {
        "region_id": "4",
        "region_name": "west",
        "provinces_id_n": {
            "Tak": "50", 
            "Ratchaburi": "55", 
            "Kanchanaburi": "56", 
            "Phetchaburi": "61", 
            "Prachuap Khiri Khan": "62"
        }
    },
    "region_d5": {
        "region_id": "5",
        "region_name": "east",
        "provinces_id_n": {
            "Chon Buri": "11", 
            "Rayong": "12", 
            "Chanthaburi": "13", 
            "Trat": "14", 
            "Chachoengsao": "15", 
            "Prachin Buri": "16", 
            "Sa Kaeo": "18"
        }
    },
    "region_d6": {
        "region_id": "6",
        "region_name": "South",
        "provinces_id_n": {
            "Nakhon Si Thammarat": "63", 
            "Krabi": "64", 
            "Phangnga": "65", 
            "Phuket": "66", 
            "Surat Thani": "67",
            "Ranong": "68", 
            "Chumphon": "69", 
            "Songkhla": "70", 
            "Satun": "71", 
            "Trang": "73", 
            "Phatthalung": "73", 
            "Pattani": "74", 
            "Yala": "75", 
            "Narathiwat": "76"
        }
    }
}

# COMMAND ----------

from pyspark.sql import Row
rows = []

for row in region_data.collect():
    region_id = row['region_id']
    region_name = row['name_eng']
    new_row = Row(region_id=region_id, region_name=region_name)
    rows.append(new_row)

region_df = spark.createDataFrame(rows)
region_df = region_df.withColumnRenamed("region_name", "name_eng") 
region_df.createOrReplaceTempView('region_df')

update_region_query = """
    MERGE INTO default.region AS target
    USING region_df AS source
    ON source.region_id = target.region_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
"""
spark.sql(update_region_query)

# COMMAND ----------

def find_region_info(station_data, region_data):
    station_id = station_data['stationID']
    area_eng = station_data['areaEN']

    parts = area_eng.split(', ')

    if len(parts) >= 3:
        station = parts[0] + ', ' + parts[1] 
        province = parts[2].rstrip(' ')
    else:
        return None 

    for region_key, region_value in region_data.items():
        if province in region_value['provinces_id_n']:
            province_id = region_value['provinces_id_n'][province]
            region_id = region_value['region_id']
            return station_id, station, province, province_id, region_id

    return None

# COMMAND ----------

for station_id in results:
    result = find_region_info(results[station_id], region_data)
    if result:
        df_to_upsert = spark.createDataFrame([result], schema=["station_code", "station", "province_eng", "province_id","region_id"])

        df_to_upsert.createOrReplaceTempView('upsert_d')
        # Update_region_province =  f"""
        #     MERGE INTO default.provinces AS target
        #     USING upsert_d AS source
        #     ON source.station_code = target.station_code
        #     WHEN MATCHED THEN UPDATE SET *
        #     WHEN NOT MATCHED THEN INSERT *
        #     """
        # spark.sql(Update_region_province)
    else:
        print(f"Province not found or incorrect area_eng format for station: {station_id}. area_eng: {area_eng}")

# COMMAND ----------

for station_id in results:

# COMMAND ----------

def find_province_id(province_name):
    for region in region_data.values():
        if province_name in region["provinces"]:
            return region["region_id"]
    return None

# COMMAND ----------

Update_region_province =  """select * from default.provinces"""
spark.sql(Update_region_province).show()

# COMMAND ----------

def find_first_comma(text):
    for i in range(len(text)):
        if text[i] == ',':
            return i
    return -1  # If no comma is found

text = "Wat Mai Subdistrict, Mueang District, Chanthaburi"  
result = find_first_comma(text)
print(result)  # Output: 19


# COMMAND ----------

d_station['areaEN'][d_station['areaEN'].find(',', d_station['areaEN'].find(',') + 1):].replace(', ', '')

# COMMAND ----------

d_station['areaEN'][:d_station['areaEN'].find(',')]


# COMMAND ----------

d_station['areaEN'][d_station['areaEN'].find(',')+2:]

# COMMAND ----------

d_station['areaEN']

# COMMAND ----------

date = []
data_pm = []
provinces = []
region = []

for station_id in station_ids:
    data = fetch_station_data(base_url, station_id, headers)
    
    stationid = data['stationID']
    name_eng = data['nameEN']
    area_eng = data['areaEN']

    date_ori = data['AQILast']['date']
    # date_con = datetime.strptime(date_ori, '%Y-%m-%d')
    # year = date_con.year
    # month = date_con.month
    # day = date_con.day
    
    # aqi = data['AQILast']['PM25']['aqi']
    # new_date = date_con.strftime('%d-%m-%Y')
    
    # date.append((year, month, day, new_date))
    # provinces.append((stationid, name_eng, area_eng))
    # data_pm.append((stationid, new_date, aqi))

# COMMAND ----------

raw_date = spark.createDataFrame(date)
raw_data_pm = spark.createDataFrame(data_pm)

# raw_provinces = spark.createDataFrame(provinces)
# raw_region = spark.createDataFrame(region)

# COMMAND ----------

raw_data_pm.show()

# COMMAND ----------

raw_date = raw_date.toDF('yyyy', 'mm', 'dd', 'data_period') 
raw_data_pm = raw_data_pm.toDF('stationid', 'data_period', 'aqi')

# COMMAND ----------

data['AQILast']

# COMMAND ----------

from datetime import datetime

# Assuming the date string is in a format like '2024-04-08'
date_string = data['AQILast']['date']
date_object = datetime.strptime(date_string, '%Y-%m-%d')
year = date_object.year


# COMMAND ----------


