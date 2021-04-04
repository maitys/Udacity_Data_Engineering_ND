import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *

# ######################
# process_song_file
# ######################
def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, typ="series")
    df = pd.DataFrame([df])
    
    # fix columns
    df.columns = ["num_songs", "artist_id", "artist_latitude", "artist_longitude", "artist_location", "artist_name", "song_id", "title", "duration", "year"]
    df["num_songs"] = df["num_songs"].astype(int)
    df["artist_id"] = df["artist_id"].astype(str)
    df["artist_latitude"] = df["artist_latitude"].astype(float)
    df["artist_longitude"] = df["artist_longitude"].astype(float)
    df["artist_location"] = df["artist_location"].replace(r'', np.NaN)
    df["artist_location"] = df["artist_location"].astype(str)
    df["artist_name"] = df["artist_name"].astype(str)
    df["song_id"] = df["song_id"].astype(str)
    df["title"] = df["title"].astype(str)
    df["duration"] = df["duration"].astype(str)
    # df["year"] = df["year"].replace(0, np.NaN)
    df["year"] = df["year"].astype(int)

    # insert song record
    song_data = list(df[["song_id", "title", "artist_id", "year", "duration"]].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0])
    cur.execute(artist_table_insert, artist_data)

# ######################
# process_log_file
# ######################
def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)
    
    # fix columns
    df.columns = ['artist', 'auth', 'firstName', 'gender', 'itemInSession', 'lastName', 'length', 'level', 'location', 'method', 'page','registration', 'sessionId', 'song', 'status', 'ts', 'userAgent', 'userId']
    df["length"] = df["length"].astype(str)
    df["registration"] = df["registration"].astype(float)
    df["sessionId"] = df["sessionId"].astype(int)
    df["status"] = df["status"].astype(int)
    # df["ts"] = df["ts"].astype(int)
    df = df.sort_values(by="ts").reset_index(drop=True)
    df["userId"] = df["userId"].replace(r"", 0)
    df["userId"] = df["userId"].astype(int)
    df["ts"] = pd.to_datetime(df['ts'], unit='ms') # .dt.tz_localize('UTC').dt.tz_convert('America/New_York')

    # filter by NextSong action
    df = df[df["page"]=="NextSong"]

    # insert time data records
    time_df = df[["ts"]].copy()
    time_df.columns = ["start_time"]
    time_df["hour"] = time_df["start_time"].dt.hour
    time_df["day"] = time_df["start_time"].dt.day
    time_df["week"] = time_df["start_time"].dt.isocalendar().week
    time_df["month"] = time_df["start_time"].dt.month
    time_df["year"] = time_df["start_time"].dt.year
    time_df["weekday"] = time_df["start_time"].dt.weekday
    #t["day_name"] = t["start_time"].dt.day_name()
    time_df = time_df.sort_values(by="start_time").reset_index(drop=True)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]].copy()
    user_df.columns = ["user_id", "first_name", "last_name", "gender", "level"]
    user_df = user_df.drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    print(filepath)

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()