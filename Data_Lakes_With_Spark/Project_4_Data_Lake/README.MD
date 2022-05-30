
- [**Project 4 - Data Lakes**](#project-4---data-lakes)
  - [**Objective:**](#objective)
  - [**How to achieve the Objective?**](#how-to-achieve-the-objective)
  - [**Info on Input Files**](#info-on-input-files)
    - [**S3 Location:**](#s3-location)
    - [**Notes:**](#notes)
  - [**Info on Output files in S3**](#info-on-output-files-in-s3)
    - [**Dimension Files** <br>](#dimension-files-)
    - [**Fact File** <br>](#fact-file-)
  - [**How to run the ETL Pipeline** <br>](#how-to-run-the-etl-pipeline-)
  - [**Files in Repo** <br>](#files-in-repo-)
  - [**Screenshots** <br>](#screenshots-)


# **Project 4 - Data Lakes**

## **Objective:**
With the growth of Sparkify, the company has moved its processes and data onto the AWS cloud. The data now resides in S3, across multiple directories of JSON logs. Main objective of this project is to make this data accessible for the analytics team so they can continue finding insights on user behavior.

## **How to achieve the Objective?**
In order to achieve this objective, we are tasked with building an ETL pipeline that extracts their data from S3, prcoesses them using Spark, and loads them back into S3 as a set of dimensional tables.

---
## **Info on Input Files**

###  **S3 Location:**
1. Song data: `s3://udacity-dend/song_data`
2. Log data: `s3://udacity-dend/log_data`

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
## **Info on Output files in S3**
Using the song and event datasets, we need to setup the following files and push them to S3 as parquet files.

### **Dimension Files** <br>
1. `users` [user_id, first_name, last_name, gender, level] --> Table with user info. Data input is from log_data
2. `songs` [song_id, title, artist_id, year, duration] --> Table with song info. Data input is from song_data
3. `artists` [artist_id, name, location, latitude, longitude] --> Table with artist info. Data input is from song_data
4. `time` [start_time, hour, day, week, month, year, weekday] --> Table with timestamps for records in `songplays` brown into specific units. Data input is from log_data

### **Fact File** <br>
1. `songplays` [start_time, user_id, level, song_id, artist_id, session_id, location, user_agent] --> Records in event data associated with song plays. Data input is from log_data.

---
## **How to run the ETL Pipeline** <br>
In order to run the ETL pipeline, we need to setup the Spark environment and create the SparkSession. This can be done by setting up an EMR cluster or via setting up Spark on a local machine. For this project, we will be using the EMR cluster.

1. Setup EMR Cluster (using emr 5.20.0)
2. Conduct testing for ETL Pipeline to ensure it works as expected. For this, we process a subset of the data via notebook on the EMR cluster. The test notebook is attached for reference
3. Update `dl.cfg` using your credentials. This file is not pushed to repo for security. Below is the info required in this config file. This file is only required if running the ETL pipeline locally.
    ```
    [AWS]
    KEY=
    SECRET=
    ```
4. Move `etl.py` to a S3 bucket. We are using `s3://smaity-spark-scripts/`
5. SSH into EMR cluster through Putty
6. Run `spark-submit s3://smaity-spark-scripts/etl.py`
7. Outputs go to `s3://maitys-sparkify-outputs/`

---
## **Files in Repo** <br>
- `etl_testing.py`: Contains the code for testing the ETL pipeline. All work in the notebook was conducted on EMR cluster.
- `dl.cfg`: Contains the credentials for AWS S3.
- `etl.py` - This file contains the logic to run the ETL pipeline

---

## **Screenshots** <br>
EMR Cluster Setup:

<img src = ".\images\cluster_specs.png" style="width:300px;"/>

EMR Notebook Specs:

<img src = ".\images\notebook_specs.png" style="width:300px;"/>

S3 ETL File Bucket:

<img src = ".\images\etl_file.png" style="width:300px;"/>

S3 Output Bucket:

<img src = ".\images\output_files.png" style="width:300px;"/>

---

