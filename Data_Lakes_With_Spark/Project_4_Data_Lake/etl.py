##########################
#### Import Libraries ####
##########################
import configparser
from datetime import datetime
import os, time
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
from pyspark.sql.types import TimestampType, DateType, IntegerType
from pyspark.sql.functions import monotonically_increasing_id

##############################
#### Create Spark Session ####
##############################
config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))
os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

###########################
#### Process Song Data ####
###########################
def process_song_data(spark, input_data, output_data):
    """
    1. Load data from song_data json files on s3
    2. Create songs and artists tables
    3. Write songs and artists tables to parquet files in S3
    
    Parameters
    ----------
    spark: Spark session
    input_data: Path to song_data json files on S3
    output_data: Path to write parquet files to S3
    """
    
    # Load raw data
    input_song_data = input_data + 'song_data/A/B/C/*.json'
    start_time = time.time()
    df_song_data = spark.read.json(input_song_data)
    print("Total time to run: {} seconds".format(round((time.time() - start_time),4)))
    print("*" * 50)
        
    # ------------------------- # 
    #### Setup songs table ####
    # ------------------------- #
    # Create table
    df_songs_table = df_song_data.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()

    # Write table to s3 --> parquet files partitioned by year and artist
    output_songs_data = output_data + 'songs/songs.parquet'
    df_songs_table.write.partitionBy('year', 'artist_id').parquet(output_songs_data, 'overwrite')
    
    # --------------------------- # 
    #### Setup artists table ####
    # --------------------------- #
    # Create table
    df_artists_table = df_song_data.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').dropDuplicates()

    # Rename columns
    column_names = ["artist_id", "name", "location", "latitude", "longitude"]
    df_artists_table = df_artists_table.toDF(*column_names)

    # Write table to s3 --> parquet files
    output_artists_data = output_data + 'artists/artists.parquet'
    df_artists_table.write.parquet(output_artists_data, 'overwrite')
    
###########################
#### Process Song Data ####
###########################
def process_log_data(spark, input_data, output_data):
    """
    1. Load data from log_data and song_data json files on s3
    2. Create users, time and songplays tables
    3. Write users, time and songplays tables to parquet files in S3
    
    Parameters
    ----------
    spark: Spark session
    input_data: Path to song_data json files on S3
    output_data: Path to write parquet files to S3
    """
    # Load raw data
    input_log_data = input_data + 'log_data/2018/11/*.json'
    input_song_data = input_data + 'song_data/A/B/C/*.json'
    start_time = time.time()
    df_log_data = spark.read.json(input_log_data)
    df_song_data = spark.read.json(input_song_data)
    print("Total time to run: {} seconds".format(round((time.time() - start_time),4)))
    print("*" * 50)
    
    # ----------------------- # 
    #### Setup users table ####
    # ----------------------- #
    # Create table
    df_users_table = df_log_data.where((df_log_data.page == 'NextSong') & (df_log_data.userId.isNotNull()))
    df_users_table = df_users_table.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()

    # Rename columns
    column_names = ["user_id", "first_name", "last_name", "gender", "level"]
    df_users_table = df_users_table.toDF(*column_names)

    # Fix Datatypes
    df_users_table = df_users_table.withColumn("user_id",col("user_id").cast('integer'))

    # Write table to s3 --> parquet files partitioned by level
    output_users_data = output_data + 'users/users.parquet'
    df_users_table.write.parquet(output_users_data, 'overwrite')

    # ---------------------- # 
    #### Setup time table ####
    # ---------------------- #
    # Create table
    df_time_table = df_log_data.where((df_log_data.page == 'NextSong') & (df_log_data.ts.isNotNull()))
    df_time_table = df_time_table.select('ts').dropDuplicates()

    # convert unix time to timestamp
    func_ts = udf(lambda ts: datetime.fromtimestamp(ts/1000).isoformat())
    df_time_table = df_time_table.withColumn('start_time', func_ts('ts').cast(TimestampType()))

    # add columns
    df_time_table = df_time_table.withColumn('hour', hour('start_time'))
    df_time_table = df_time_table.withColumn('day', dayofmonth('start_time'))
    df_time_table = df_time_table.withColumn('week', weekofyear('start_time'))
    df_time_table = df_time_table.withColumn('month', month('start_time'))
    df_time_table = df_time_table.withColumn('year', year('start_time'))
    df_time_table = df_time_table.withColumn('weekday', dayofweek('start_time'))

    # remove columns
    df_time_table = df_time_table.drop('ts')

    # Write table to s3 --> parquet files partitioned by year and month
    output_time_data = output_data + 'time/time.parquet'
    df_time_table.write.partitionBy('year', 'month').parquet(output_time_data, 'overwrite')
    
    # --------------------------- # 
    #### Setup songplays table ####
    # --------------------------- #
    # Create table
    df_songplays_table = df_log_data.join(df_song_data, 
                                          (df_log_data.song == df_song_data.title) & (df_log_data.artist == df_song_data.artist_name), 
                                          how='inner')
    # Filter
    df_songplays_table = df_songplays_table.where((df_songplays_table.page == 'NextSong') & (df_songplays_table.ts.isNotNull()))

    # convert unix time to timestamp
    func_ts = udf(lambda ts: datetime.fromtimestamp(ts/1000).isoformat())
    df_songplays_table = df_songplays_table.withColumn('start_time', func_ts('ts').cast(TimestampType()))

    # Add songplay_id
    df_songplays_table = df_songplays_table.withColumn('songplay_id', monotonically_increasing_id())

    # Add columns
    df_songplays_table = df_songplays_table.withColumn('month', month('start_time'))
    df_songplays_table = df_songplays_table.withColumn('year', year('start_time'))

    # Select required columns
    df_songplays_table = df_songplays_table.select('songplay_id', 'start_time', 'userId', 'level', 
                                                   'song_id', 'artist_id', 'sessionId', 'location', 'userAgent', 'month', 'year').dropDuplicates()

    # Rename columns
    column_names = ['songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent', 'month', 'year']
    df_songplays_table = df_songplays_table.toDF(*column_names)

    # Write table to s3 --> parquet files partitioned by year and month
    output_songplays_data = output_data + 'songplays/songplays.parquet'
    df_songplays_table.write.partitionBy('year', 'month').parquet(output_songplays_data, 'overwrite')

##############
#### MAIN ####
##############
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://maitys-sparkify-outputs/"
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
