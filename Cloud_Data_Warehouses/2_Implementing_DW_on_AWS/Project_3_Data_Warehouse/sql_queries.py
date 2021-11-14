#####################################
#### LOAD PACKAGES###################
#####################################
import pandas as pd
import boto3
import json
import time
import configparser
import psycopg2

#####################################
#### CONFIG #########################
#####################################
config = configparser.ConfigParser()
config.read('maitys_aws_dwh.cfg')
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")
DWH_ENDPOINT           = config.get("DWH","DWH_ENDPOINT")
DWH_ROLE_ARN           = config.get("DWH","DWH_ROLE_ARN")
LOG_DATA               = config.get("S3","LOG_DATA")
LOG_JSONPATH           = config.get("S3","LOG_JSONPATH")
SONG_DATA             = config.get("S3","SONG_DATA")
AWS_REGION             = config.get("S3","AWS_REGION")
DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

#####################################
#### DROP TABLES IF EXIST ###########
#####################################
staging_events_table_drop = "DROP TABLE IF EXISTS public.staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS public.staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS public.songplays;"
user_table_drop = "DROP TABLE IF EXISTS public.users;"
song_table_drop = "DROP TABLE IF EXISTS public.songs;"
artist_table_drop = "DROP TABLE IF EXISTS public.artists;"
time_table_drop = "DROP TABLE IF EXISTS public.time;"

#####################################
#### CREATE TABLES ##################
#####################################
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS public.staging_events (
    artist TEXT, 
    auth TEXT, 
    firstName TEXT, 
    gender TEXT, 
    ItemInSession TEXT,
    lastName TEXT, 
    length FLOAT8, 
    level TEXT, 
    location TEXT, 
    method TEXT,
    page TEXT, 
    registration TEXT, 
    sessionId TEXT, 
    song TEXT, 
    status TEXT,
    ts BIGINT, 
    userAgent TEXT, 
    userId TEXT);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS public.staging_songs (
    artist_id TEXT,
    artist_latitude FLOAT8,
    artist_longitude FLOAT8, 
    artist_location TEXT, 
    artist_name TEXT,
    duration FLOAT8, 
    num_songs INT, 
    song_id TEXT PRIMARY KEY, 
    title TEXT, 
    year INT);
""")

songplay_table_create = ("""
CREATE TABLE public.songplays(
    songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id TEXT NOT NULL DISTKEY,
    level TEXT,
    song_id TEXT NOT NULL,
    artist_id TEXT NOT NULL,
    session_id TEXT,
    location TEXT,
    user_agent TEXT);
""")

user_table_create = ("""
CREATE TABLE public.users(
    user_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    level TEXT)
    DISTSTYLE AUTO;                    
""")

song_table_create = ("""
CREATE TABLE public.songs(
    song_id TEXT PRIMARY KEY,
    title TEXT,
    artist_id TEXT NOT NULL DISTKEY,
    year INTEGER,
    duration FLOAT8);
""")

artist_table_create = ("""
CREATE TABLE public.artists(
    artist_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    location TEXT,
    lattitude FLOAT8,
    longitude FLOAT8)
    DISTSTYLE AUTO;
""")

time_table_create = ("""
CREATE TABLE public.time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER, 
    day INTEGER, 
    week INTEGER, 
    month INTEGER, 
    year INTEGER, 
    weekday INTEGER)
    DISTSTYLE AUTO;
""")

#####################################
#### STAGING TABLES #################
#####################################
staging_events_copy = ("""
copy public.staging_events from '{}'
credentials 'aws_iam_role={}'
compupdate off region '{}'
timeformat as 'epochmillisecs'
truncatecolumns blanksasnull emptyasnull
json '{}';                                             
""").format(LOG_DATA, DWH_ROLE_ARN, AWS_REGION, LOG_JSONPATH)

staging_songs_copy = ("""
copy public.staging_songs from '{}'
credentials 'aws_iam_role={}'
format as json 'auto' compupdate off region '{}';
""").format(SONG_DATA, DWH_ROLE_ARN, AWS_REGION)

#####################################
#### FINAL TABLES ###################
#####################################
songplay_table_insert = ("""
INSERT INTO public.songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT  TIMESTAMP 'epoch' + a.ts/1000 * INTERVAL '1 second' AS start_time, 
        a.userId as user_id,
        a.level,
        b.song_id,
        b.artist_id,
        a.sessionId as session_id,
        a.location,
        a.userAgent as user_agent
FROM    staging_events AS a 
JOIN    staging_songs AS b
ON      a.artist = b.artist_name
AND     a.length = b.duration
AND     a.song = b.title
WHERE   a.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO public.users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT 
            userId as user_id,
            firstName as first_name,
            lastName as last_name,
            gender,
            level
FROM        public.staging_events
WHERE       page = 'NextSong'
AND         userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO public.songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,    
    duration
FROM public.staging_songs;                     
""")

artist_table_insert = ("""
INSERT INTO public.artists(artist_id, name, location, lattitude, longitude)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,    
    artist_longitude
FROM public.staging_songs;
""")

time_table_insert = ("""
INSERT INTO public.time (start_time, hour, day, week, month, year, weekday) 
Select DISTINCT 
    start_time,
    EXTRACT(HOUR FROM start_time) As hour,
    EXTRACT(DAY FROM start_time) As day,
    EXTRACT(WEEK FROM start_time) As week,
    EXTRACT(MONTH FROM start_time) As month,
    EXTRACT(YEAR FROM start_time) As year,
    EXTRACT(DOW FROM start_time) As weekday
FROM (SELECT DISTINCT ts,'1970-01-01'::date + ts/1000 * interval '1 second' AS start_time FROM public.staging_events);
""")

#####################################
#### QUERY LISTS ####################
#####################################
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
