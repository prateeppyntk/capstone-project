# Build Data Analysis Pipeline for PM2.5 in Thailand

**Table of Contents**

* [Problem](#problem)
* [Datasets](#datasets)
* [Data Modeling](#data-modeling)
* [Data Dictionary](#data-dictionary)
* [Data Quality Checks](#data-quality-checks)
* [Data infrastructure](#data-infrastructure)
* [Technologies](#technologies)
* [Files and What They Do](#files-and-what-they-do)
* [Instruction on Running the Project](#instruction-on-running-the-project)


## Problem



## Datasets

* ข้อมูล PM2.5 ในประเทศไทย [Air4Thai: Regional Air Quality and Situation Reports](http://www.air4thai.com/webV3/#/History)
* ข้อมูลไฟป่าในประเทศไทย [Air4Thai: Regional Air Quality and Situation Reports](http://www.air4thai.com/webV3/#/History)
* ข้อมูลโรงงานอุตสาหกรรมในประเทศไทย [Air4Thai: Regional Air Quality and Situation Reports](http://www.air4thai.com/webV3/#/History)

## Data Modeling



## Data Dictionary

### stations

| Name | Type | Description |
| - | - | - |
| station_code | varchar | ID of station (primary key) |
| station_name | varchar | Name of station |
| station_address | varchar | Address of station |
| lat | varchar | Latitude of station |
| long | varchar | Longitude of station |
| province_id | varchar | ID of province (foreign key) |

### provinces

| Name | Type | Description |
| - | - | - | 		
| province_id | varchar | ID of province (primary key) |
| province_name | varchar | Name of province |
| region_id | varchar | ID of region (foreign key) |

### region

| Name | Type | Description |
| - | - | - | 		
| region_id | varchar | ID of region (primary key) |
| region_name | varchar | Name of region |

### burned_areas

| Name | Type | Description |
| - | - | - | 		
| year | date | Year of burned (primary key) |
| province_id | varchar | ID of province (foreign key) |
| burned_area | integer | Area of burned in each province |

### province_factories 

| Name | Type | Description |
| - | - | - | 		
| province_id | varchar | ID of province (primary key) |
| factory_type_id | varchar | ID of factory type (primary key) |
| province_factory_area | integer | Area of factory in each province |

### factory_types

| Name | Type | Description | 	
| - | - | - | 		
| factory_type_id | varchar | ID of factory type (primary key) |
| factory_type_name | varchar | Name of factory type |


## Data Quality Checks

To ensure the data quality, we'll run the data quality checks to make sure that

* Column `dt` in table `global_temperature` should *not* have NULL values
* Column `AverageTemperature` in table `global_temperature` should *not* have values greater than 100
* Table `worldbank` should have records
* Column `value` in table `worldbank` should *not* have NULL values

## Data Infrastructure



## Technologies

* Google Sheet: ใช้สำหรับจัดเก็บข้อมูลดิบที่ใช้ในการเริ่มต้น
* Databricks: ใช้สำหรับเป็น Data Lake, Data Warehouse, และ Data Ingestion เพื่อดึงข้อมูล API ตาม schedule อัตโนมัติในแต่ละวัน
* PySpark: ใช้สำหรับการประมวลผลข้อมูล
* Power BI:  ใช้สำหรับเป็น BI Tool
  
## Files and What They Do

| Name | Description |
| - | - |
| `README.md` | README file that provides discussion on this project |
| `mnt/dags/climate_change_with_worldbank_data_pipeline.py` |  |
| `mnt/plugins/` |  |
| `mnt/plugins/` |  |

## Instruction on Running the Project

1. Google Sheet
  1. เปิดไฟล์ pm.csv

2. Databricks
  1.
  
3. Power BI
  1. เลือก Get Data ด้วย Azure Databricks
  2. กรอก Server Hostname และ HTTP Path
  3. กรอก Token

