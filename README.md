# Build Data Analysis Pipeline for PM2.5 in Thailand

**Table of Contents**

* [Problem](#problem)
* [Datasets](#datasets)
* [Data Modeling](#data-modeling)
* [Data Dictionary](#data-dictionary)
* [Data infrastructure](#data-infrastructure)
* [Technologies](#technologies)
* [Dashboard](#dashboard)
* [Files and What They Do](#files-and-what-they-do)
* [Instruction on Running the Project](#instruction-on-running-the-project)


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
![Data Modeling_Raw](data_model_raw.png)

## Data Dictionary

### pm2.5

| Name | Type | Description |
| - | - | - |
| date | date | Measurement date (primary key) |
| station_code | varchar | ID of station (foreign key) |
| value | float | Measured value |

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


## Data Pipeline

![Data Pipeline](data_pipeline.jpg)


## Technologies

* Google Sheet: ใช้สำหรับจัดเก็บข้อมูลดิบที่ใช้ในการเริ่มต้น
* Databricks: ใช้สำหรับเป็น Data Warehouse และ Data Ingestion เพื่อดึงข้อมูล API ตาม schedule อัตโนมัติในแต่ละวัน
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
[Go To Instruction](https://docs.google.com/document/d/1dCnek2Kl9YsPJZxKuAzpLUOfrXD9gWWcaM2maEY4A6g/edit)


