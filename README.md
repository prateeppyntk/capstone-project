# Build Data Analysis Pipeline for PM2.5 in Thailand

**Table of Contents**

* [Project Document](#project_document)
* [Problem](#problem)
* [Datasets](#datasets)
* [Data Modeling](#data-modeling)
* [Data Dictionary](#data-dictionary)
* [Data Pipeline](#data-pipeline)
* [Technologies](#technologies)
* [Dashboard](#dashboard)
* [Files and What They Do](#files-and-what-they-do)
* [Instruction on Running the Project](#instruction-on-running-the-project)

## Project Document
[Go To Document](https://docs.google.com/document/d/1dCnek2Kl9YsPJZxKuAzpLUOfrXD9gWWcaM2maEY4A6g/edit)

## Problem

PM2.5 หรือชื่อเต็มคือ Particulate Matter with diameter of less than 2.5 micron เป็นฝุ่นละอองขนาดจิ๋วที่มีขนาดไม่เกิน 2.5 ไมครอน ซึ่งมีขนาดประมาณ 1 ใน 25 ส่วนของเส้นผ่าศูนย์กลางเส้นผมมนุษย์ โดยมีขนาดเล็กจนขนจมูกของมนุษย์ที่ทำหน้าที่กรองฝุ่นนั้นไม่สามารถกรองได้ จึงทำให้ฝุ่น PM2.5 สามารถแพร่กระจายเข้าสู่ทางเดินหายใจ กระแสเลือด และเข้าสู่อวัยวะอื่นๆ ในร่างกายได้

PM2.5 ได้เริ่มมีบทบาทในประเทศไทยอย่างมากในช่วงต้นปี 2562 ในตอนนั้นประเทศไทยประสบกับสภาวะฝุ่นปกคลุมอย่างหนาแน่น ทำให้ทุกภาคส่วนทั้งรัฐบาล เอกชน ตลอดจนประชาชน ล้วนให้ความสนใจเป็นอย่างมากพร้อมกับหาคำตอบว่าเกิดอะไรขึ้น จนสุดท้ายก็ทำให้เราทุกคนได้รู้จักกับค่าฝุ่น PM2.5 นี้ นับตั้งแต่นั้นมาจึงมีการตรวจวัดปริมาณค่าฝุ่น PM2.5 ตามจุดบริเวณต่างๆ ของประเทศไทย เพื่อตรวจสอบว่าเป็นอันตรายต่อสุขภาพหรือไม่

PM2.5 สามารถเกิดขึ้นได้จากหลายปัจจัย ทั้งจากกระบวนการผลิตในโรงงานอุตสาหกรรม จากการเผาไหม้ในกิจกรรมในครัวเรือน จากการเผาทางการเกษตร จากการเผาป่า จาการการจราจร รวมถึงสาเหตุอื่น เช่น การรวมตัวของก๊าซอื่นๆ ในบรรยากาศ 

ด้วยเหตุนี้ทำให้ทางกลุ่มของเรามีความสนใจที่จะศึกษาเกี่ยวกับแนวโน้มของปริมาณค่าฝุ่น PM2.5 ในประเทศไทยตามบริเวณต่างๆ รวมถึงต้องการศึกษาว่า ปัจจัยที่ส่งผลต่อแนวโน้มของปริมาณค่าฝุ่น PM2.5 มีลักษณะเด่นเป็นอย่างไรในแต่ละภูมิภาค โดยในโปรเจคนี้ขอทำการศึกษาทั้งหมด 2 ปัจจัย ได้แก่ 1.โรงงานอุตสาหกรรม: พิจารณาจากพื้นที่ของโรงงานอุตสาหกรรมในแต่ละภูมิภาค และ 2.ไฟป่า: พิจารณาพื้นที่ที่เกิดไฟป่าในภูมิภาคนั้นๆ

โดยการใช้งานจริงนั้นคาดว่าสามารถใช้แนวโน้มค่าฝุ่นที่เปลี่ยนไปเป็นตัวชี้วัดคุณภาพการแก้ปัญหาไฟป่าและมาตรการจัดการฝุ่นละอองที่เกิดจากโรงงานอุตสาหกรรมในแต่ละภูมิภาคหรือระดับจังหวัด


## Datasets

* ข้อมูล PM2.5 ในประเทศไทย
  <br /> [Air4Thai: Regional Air Quality and Situation Reports](http://www.air4thai.com/webV3/#/History) 
* ข้อมูลไฟป่าในประเทศไทย
  <br /> [Digital Government Development Agency:  Open Government Data of Thailand](https://data.go.th/dataset/gdpublish-fire1) 
* ข้อมูลโรงงานอุตสาหกรรมในประเทศไทย
  <br /> [Department of Industrial Works:  บัญชีประเภทโรงงานอุตสาหกรรม](https://www.diw.go.th/datahawk/factype.php)
  <br /> [Department of Industrial Works:  พื้นที่โรงงานอุตสาหกรรม](https://www.diw.go.th/datahawk/factype.php)

## Data Modeling
ในโปรเจคได้มีการสร้าง Data Warehouse ทั้งหมด 2 ที่ โดยแบ่งเป็น Data Warehouse ที่จัดเก็บข้อมูลดิบ (raw data) และ Data Warehouse ที่จัดเก็บข้อมูลที่ถูก Transfrom และสร้างเป็น view สำหรับนำไปวิเคราะห์ต่อในการทำ visualization 

* Data Warehouse:  Stored raw data
![Data Modeling_Raw](data_model_raw.png)

* Data Warehouse:  Stored analyzed data


## Data Dictionary

### PM2.5

| Name | Type | Description |
| - | - | - |
| station_code | varchar | ID of station (primary key) |
| date | date | Measurement date (primary key) |
| province_id | int | ID of province (foreign key) |
| value | int | Measured value |
| level | int | Air quality level |

### Dates

| Name | Type | Description |
| - | - | - |
| date | date | Measurement date (primary key) |
| year | int | Measurement year (primary key) |
| month | int | Measurement month |
| month_name | varchar | Measurement month's name |

### provinces

| Name | Type | Description |
| - | - | - | 		
| province_id | int | ID of province (primary key) |
| province_name | varchar | Name of province |
| region_id | int | ID of region (foreign key) |
| region_name | varchar | Name of region |

### burned_areas

| Name | Type | Description |
| - | - | - | 		
| year | int | Measurement year (primary key) |
| date | date | Measurement date (primary key) |
| province_id | int | ID of province (foreign key) |
| burned_area | decimal | Area of burned in each province |

### province_factories 

| Name | Type | Description |
| - | - | - | 		
| factory_type_id | int | ID of factory type (primary key) |
| province_id | int | ID of province (primary key) |
| province_factory_area | decimal | Area of factory in each province |

### factory_types

| Name | Type | Description | 	
| - | - | - | 		
| factory_type_id | int | ID of factory type (primary key) |
| factory_type_name | varchar | Name of factory type |

### level

| Name | Type | Description |
| - | - | - |
| level | int | Air quality level (primary key) |
| min | int | Min of air quality level range |
| max | int | Max of air quality level range |
| description | varchar | Descripton of air quality level |

### stations

| Name | Type | Description |
| - | - | - |
| station_code | varchar | ID of station (primary key) |
| station_name | varchar | Name of station |
| station_address | varchar | Address of station |
| lat | decimal | Latitude of station |
| long | decimal | Longitude of station |

### PM2.5SummaryByStation

| Name | Type | Description |
| - | - | - | 		
| station_code | varchar | ID of station (primary key) |
| avg | decimal | Average of pm2.5 value |
| avg_level | int | Air quality level of average pm2.5 value |
| max | decimal | Max of pm2.5 value |
| max_level | int | Air quality level of max pm2.5 value |
| min | decimal | Min of pm2.5 value |
| min_level | int | Air quality level of min pm2.5 value |


## Data Pipeline

![Data Pipeline](data_pipeline.jpg)


## Technologies

* Google Sheet: ใช้สำหรับจัดเก็บข้อมูลดิบที่ใช้ในการเริ่มต้น
* Databricks: ใช้สำหรับเป็น Data Warehouse และ Data Ingestion เพื่อดึงข้อมูล API ตาม schedule ใน workflow อัตโนมัติในแต่ละวัน
* PySpark: ใช้สำหรับการประมวลผลข้อมูล
* Power BI:  ใช้สำหรับเป็น BI Tool

## Dashboard
[Go To Dashboard](https://app.powerbi.com/links/ivZ5SWlCVO?ctid=f90c4647-886f-4b4c-b2eb-555df9ec4e81&pbi_source=linkShare)


  
## Files and What They Do

| Name | Description |
| - | - |
| `README.md` | README file that provides discussion on this project |
| `data-api-upsert.py` |  |
| `mnt/plugins/` |  |
| `mnt/plugins/` |  |

## Instruction on Running the Project
[Go To Instruction](https://docs.google.com/document/d/1-7AHIKPvSPIHpQUK3y93g01lnmfENmwaGRHWCNlyhu0/edit?usp=sharing)



