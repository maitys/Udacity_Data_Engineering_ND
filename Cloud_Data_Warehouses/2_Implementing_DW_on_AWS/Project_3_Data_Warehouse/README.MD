
- [**Project 3 - Data Warehouse**](#project-3---data-warehouse)
  - [**Objective:**](#objective)
  - [**How to achieve the Objective?**](#how-to-achieve-the-objective)
  - [**Info on Input Files**](#info-on-input-files)
    - [**S3 Location:**](#s3-location)
    - [**Notes:**](#notes)
  - [**Info on Output Tables in Redshift**](#info-on-output-tables-in-redshift)
    - [**Dimension Tables** <br>](#dimension-tables-)
    - [**Fact Table** <br>](#fact-table-)
  - [**How to run the ETL Pipeline** <br>](#how-to-run-the-etl-pipeline-)
  - [**Files in Repo** <br>](#files-in-repo-)
  - [**Sample Queries** <br>](#sample-queries-)


# **Project 3 - Data Warehouse**

## **Objective:**
With the growth of Sparkify, the company has moved its processes and data onto the AWS cloud. The data now resides in S3, across multiple directories of JSON logs. Main objective of this project is to make this data accessible for the analytics team so they can continue finding insights on user behavior.

## **How to achieve the Objective?**
In order to achieve this objective, we are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables.

---
## **Info on Input Files**

###  **S3 Location:**
1. Song data: `s3://udacity-dend/song_data`
2. Log data: `s3://udacity-dend/log_data`
3. Log data json path: `s3://udacity-dend/log_json_path.json`

###  **Notes:**
1. Song Dataset
    - Files are in JSON Format and contain metadata about song, and its artists
    - Files are partitioned by the first 3 letters of each song's track ID
    - Eg:
        - `song_data/A/B/C/TRABCEI128F424C983.json`
        - `song_data/A/A/B/TRAABJL12903CDCF1A.json`
        - `{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}`

1. Log Dataset:
    - App activity logs from an imaginary music streaming company
    - JSON Files partitioned by year and month
    - Eg:
        - `log_data/2018/11/2018-11-12-events.json`
        - `log_data/2018/11/2018-11-13-events.json`
---
## **Info on Output Tables in Redshift**
Using the song and event datasets, we need to create a star schema optimized for queries on song play analysis. This includes the following tables.

### **Dimension Tables** <br>
1. `users` [user_id, first_name, last_name, gender, level] --> Table with user info. Data input is from log_data
2. `songs` [song_id, title, artist_id, year, duration] --> Table with song info. Data input is from song_data
3. `artists` [artist_id, name, location, latitude, longitude] --> Table with artist info. Data input is from song_data
4. `time` [start_time, hour, day, week, month, year, weekday] --> Table with timestamps for records in `songplays` brown into specific units. Data input is from log_data

### **Fact Table** <br>
1. `songplays` [start_time, user_id, level, song_id, artist_id, session_id, location, user_agent] --> Records in event data associated with song plays. Data input is from log_data.

---
## **How to run the ETL Pipeline** <br>
1. Install required packages from `requirements.txt`
    >pip install -r requirements.txt
2. Update `maitys_aws_dwh.cfg` using your credentials. This file is not pushed to repo for security. Below is the info required in this config file
    ```
    [AWS]
    KEY=
    SECRET=

    [DWH] 
    DWH_CLUSTER_TYPE=multi-node
    DWH_NUM_NODES=4
    DWH_NODE_TYPE=dc2.large

    DWH_IAM_ROLE_NAME=
    DWH_CLUSTER_IDENTIFIER=
    DWH_DB=
    DWH_DB_USER=
    DWH_DB_PASSWORD=
    DWH_PORT=5439
    DWH_ENDPOINT=
    DWH_ROLE_ARN=

    [S3]
    LOG_DATA=s3://udacity-dend/log_data
    LOG_JSONPATH=s3://udacity-dend/log_json_path.json
    SONG_DATA=s3://udacity-dend/song_data
    AWS_REGION=us-west-2
    ```
3. Run `python create_tables.py` to create the database and required tables
4. Run `python etl.py` to run the ETL piepline 
   
---
## **Files in Repo** <br>
- `sql_queries.py`: Contains the SQL queries to drop tables, create the tables, and load data into tables
- `create_tables.py` -  This file contains the logic to create the database and required tables
- `etl.py` - This file contains the logic to run the ETL pipeline
- `nb1_resource_setup.ipynb` - This file setups the required resouces for the ETL pipeline
- `nb2_data_extraction.ipynb` - This file contains the logic to extract data from S3 and stage it in Redshift
- `nb3_cleanup_resources.ipynb` - This file cleans up the resources created during the ETL pipeline
- `requirements.txt` - This file contains the required packages for the ETL pipeline

---
## **Sample Queries** <br>
```
    -- Top 10 Songs Played
    select b.title, count(*) 
    from public.songplays  a
    join public.songs b
    on a.song_id = b.song_id
    group by 1 order by 2 desc limit 10;

    -- Songs Plays by day for top played title: You're The One
    select cast(start_time as date) as date, b.title, count(distinct a. song_id) as plays
    from public.songplays a
    join public.songs b
    on a.song_id = b.song_id
    where b.title = 'You\'re The One'
    group by 1,2 order by 1,2;

    -- Top 10 Artists Played
    select b.name, count(*) 
    from public.songplays  a
    join public.artists b
    on a.artist_id = b.artist_id
    group by 1 order by 2 desc limit 10;

    -- Songs Plays by day for top played title: You're The One
    select cast(start_time as date) as date, b.name, count(distinct a.  artist_id) as plays
    from public.songplays a
    join public.artists b
    on a.artist_id = b.artist_id
    where b.name = 'Dwight Yoakam'
    group by 1,2 order by 1,2;
```

---