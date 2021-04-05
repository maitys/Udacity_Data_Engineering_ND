# Project 1 - Data Modeling with Postgres

## Objective: 
To allow business/data users at Sparkify analyze song and user activity data being collected from their new music streaming app. One of the main goals includes understanding what songs are users listening to

### How to achieve the Objective? 
Setup a Postgres database with tables designed for users to query and gain insights. We create a database schema and ETL pipeline to achieve this objective.


## Input Files:
1. song files --> Carries information with regards to song --> `Project_1_Data_Modeling_with_Postgres\data\song_data`
2. log files --> Carries information with regards to user activity --> `Project_1_Data_Modeling_with_Postgres\data\log_data`

## Postgres Tables:
1. `users` [user_id, first_name, last_name, gender, level] --> Table with user info. Data input is from log_data
2. ``songs` [song_id, title, artist_id, year, duration] --> Table with song info. Data input is from song_data
3. `artists` [artist_id, name, location, latitude, longitude] --> Table with artist info. Data input is from song_data
4. `time` [start_time, hour, day, week, month, year, weekday] --> Table with timestamps for records in `songplays` brown into specific units. Data input is from og_data
5. `songplays` [start_time, user_id, level, song_id, artist_id, session_id, location, user_agent] --> Records in log data associated with song plays. Data input is rom log_data

## How to run the ETL Pipeline:
1. Run **create.py** --> This script drops any tables in the database, recreates empty tables with specified datatypes. All the sql queries used in this script is contained in **sql_queries.py**
2. Run etl.py* --> This script loads data into the empty tables created in **create.py**

## Files in Repo:
1. **create.py** --> This script drops any tables in the database, recreates empty tables with specified datatypes
2. **sql_queries.py** --> This script contains all sql queries used to drop, create, and insert data into the postgres tabless
3. **etl.py** --> This script loads data into the empty tables created in **create.py**
4. **etl.ipynb** --> Working etl notebook to setup and test code chunks that go in **etl.py**
5. **test.py** --> Used to run sql queries on tables after the ETL process is run. This is used to test if data is being populated in the tables as expected
6. **data** --> This folder contains log and song data files

## Sample Queries:
- `select * from songplays limit 5;` 
- `select * from songs limit 5;`
- `select * from songplays where song_id is not null and artist_id is not null;`

    


 

