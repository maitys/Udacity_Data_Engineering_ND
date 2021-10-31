# **Project 3 - Data Warehouse**

## **Objective:**
With the growth of Sparkify, the company has moved its processes and data onto the AWS cloud. The data now resides in S3, across multiple directories of JSON logs. Main objective of this project is to make this data accessible for the analytics team so they can continue finding insights on user behavior.

### **How to achieve the Objective?**
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
## **Info on Output Tablets**
Using the song and event datasets, we need to create a star schema optimized for queries on song play analysis. This includes the following tables.

**Dimension Tables** <br>
1. `users` [user_id, first_name, last_name, gender, level] --> Table with user info. Data input is from log_data
2. `songs` [song_id, title, artist_id, year, duration] --> Table with song info. Data input is from song_data
3. `artists` [artist_id, name, location, latitude, longitude] --> Table with artist info. Data input is from song_data
4. `time` [start_time, hour, day, week, month, year, weekday] --> Table with timestamps for records in `songplays` brown into specific units. Data input is from log_data

**Fact Table** <br>
1. `songplays` [start_time, user_id, level, song_id, artist_id, session_id, location, user_agent] --> Records in log data associated with song plays. Data input is rom log_data
---