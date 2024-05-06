# Build Data Analysis Pipeline for PM2.5 in Thailand

**Table of Contents**

* [Problem](#problem)
* [Datasets](#datasets)
* [Data Modeling](#data-modeling)
* [Data Dictionary](#data-dictionary)
* [Data Quality Checks](#data-quality-checks)
* [Data infrastructure](#data-infrastructure)
* [Technologies](#technologies)
* [Future Design Considerations](#future-design-considerations)
* [Files and What They Do](#files-and-what-they-do)
* [Instruction on Running the Project](#instruction-on-running-the-project)


## Problem



## Datasets

* ข้อมูล PM2.5 ในประเทศไทย [Air4Thai: Regional Air Quality and Situation Reports](http://www.air4thai.com/webV3/#/History)
* ข้อมูลไฟป่าในประเทศไทย [Air4Thai: Regional Air Quality and Situation Reports](http://www.air4thai.com/webV3/#/History)
* ข้อมูลโรงงานอุตสาหกรรมในประเทศไทย [Air4Thai: Regional Air Quality and Situation Reports](http://www.air4thai.com/webV3/#/History)

## Data Modeling



## Data Dictionary

### Stations

| station_code | station_name	| station_address |	lat	long |	province_id |
| - | - | - | - | - |
| dt | date | Date (as primary key) |
| AverageTemperature | decimal | Average land temperature in celsius |
| AverageTemperatureUncertainty | decimal | The 95% confidence interval around the average  |
| city | varchar(256) | City |
| country | varchar(256) | Country |
| latitude | varchar(256) | Latitude of the city |
| longitude | varchar(256) | Longitude of the city |

### World Bank Country Profile

| Name | Type | Description |
| - | - | - |
| id | int | ID (as primary key) |
| country_code | varchar(256) | Country code |
| country_name | varchar(256) | Country name |
| indicator_code | varchar(256) | Indicator code |
| indicator_name | varchar(256) | Indicator name |
| value | decimal | Value of the indicator |
| year | int | Year |

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
  
## Future Design Considerations

* The data was increased by 100x.

  In this project, we have already used Amazon EMR, which is a  cloud big data
  platform for running large-scale distributed data processing jobs. This means
  we can scale our cluster up to add the processing power when the job gets too
  slow.

  We could store the data in Parquet format instead of CSV to save disk space
  and cost. We can also partition the data by date or country, which depends on
  how we query the data to answer business questions.

* The data populates a dashboard that must be updated on a daily basis by 7am every day.

  Here using Apache Airflow can be very useful since we can schedule our
  workflow to update the data used by a dashboard on a daily basis.

* The database needed to be accessed by 100+ people.

  Amazon Redshift can handle the connections up to 500 connections by default.

## Files and What They Do

| Name | Description |
| - | - |
| `mnt/dags/climate_change_with_worldbank_data_pipeline.py` | An Airflow DAG file that runs the ETL data pipeline on climate change and world bank profile data |
| `mnt/plugins/` | An Airflow plugin folder that contains customer operators used in this project |
| `spark/app/global_temperature_data_processing.py` | A Spark app that reads the global temperature data from CSV, runs ETL, and saves data in Parquet |
| `README.md` | README file that provides discussion on this project |

## Instruction on Running the Project

Running Airflow on local machine:

```sh
cp .env.local .env
echo -e "AIRFLOW_UID=$(id -u)" >> .env
mkdir -p mnt/dags mnt/logs mnt/plugins
docker-compose build
docker-compose up
```

