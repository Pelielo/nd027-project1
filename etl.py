# # ETL Processes
import os
import glob
import psycopg2
import pandas as pd
import json
import datetime
from sql_queries import *

conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
cur = conn.cursor()

def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files


def process_song_data(cur, conn, filepath):
    # # Process `song_data`
    # In this first part, you'll perform ETL on the first dataset, `song_data`, to create the `songs` and `artists` dimensional tables.
    # 
    # Let's perform ETL on a single song file and load a single record into each table to start.
    # - Use the `get_files` function provided above to get a list of all song JSON files in `data/song_data`
    # - Select the first song in this list
    # - Read the song file and view the data

    song_files = get_files(filepath)
    print(f"song file example: {song_files[0]}")
    print(f"number of song files: {len(song_files)}")

    json_data = []

    for song in song_files:
        with open(song) as json_file:
            data = json.load(json_file)
            json_data.append(data)
            
    song_df = pd.DataFrame.from_dict(json_data)
    song_df

    # ## #1: `songs` Table
    # #### Extract Data for Songs Table
    # - Select columns for song ID, title, artist ID, year, and duration
    # - Use `df.values` to select just the values from the dataframe
    # - Index to select the first (only) record in the dataframe
    # - Convert the array to a list and set it to `song_data`

    song_data = song_df[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()

    # #### Insert Record into Song Table
    # Implement the `song_table_insert` query in `sql_queries.py` and run the cell below to insert a record for this song into the `songs` table. Remember to run `create_tables.py` before running the cell below to ensure you've created/resetted the `songs` table in the sparkify database.

    for song in song_data:
        cur.execute(song_table_insert, tuple(song))
        conn.commit()

    # ## #2: `artists` Table
    # #### Extract Data for Artists Table
    # - Select columns for artist ID, name, location, latitude, and longitude
    # - Use `df.values` to select just the values from the dataframe
    # - Index to select the first (only) record in the dataframe
    # - Convert the array to a list and set it to `artist_data`

    artist_df = song_df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = artist_df.values.tolist()

    # #### Insert Record into Artist Table
    # Implement the `artist_table_insert` query in `sql_queries.py` and run the cell below to insert a record for this song's artist into the `artists` table. Remember to run `create_tables.py` before running the cell below to ensure you've created/resetted the `artists` table in the sparkify database.

    for artist in artist_data:
        cur.execute(artist_table_insert, tuple(artist))
        conn.commit()


def process_log_data(cur, conn, filepath):
    # # Process `log_data`
    # In this part, you'll perform ETL on the second dataset, `log_data`, to create the `time` and `users` dimensional tables, as well as the `songplays` fact table.
    # 
    # Let's perform ETL on a single log file and load a single record into each table.
    # - Use the `get_files` function provided above to get a list of all log JSON files in `data/log_data`
    # - Select the first log file in this list
    # - Read the log file and view the data

    log_files = get_files(filepath)
    print(f"log file example: {log_files[0]}")
    print(f"number of log files: {len(log_files)}")

    json_data = []

    for log in log_files: 
        with open(log) as json_file:
            for line in json_file:
                data = json.loads(line)
                json_data.append(data)
                
    # df = pd.read_json(filepath, lines=True) # ALTERNATIVE 
    log_df = pd.DataFrame.from_dict(json_data)
    log_df = log_df[log_df['page'] == 'NextSong']

    # ## #3: `time` Table
    # #### Extract Data for Time Table
    # - Filter records by `NextSong` action
    # - Convert the `ts` timestamp column to datetime
    #   - Hint: the current timestamp is in milliseconds
    # - Extract the timestamp, hour, day, week of year, month, year, and weekday from the `ts` column and set `time_data` to a list containing these values in order
    #   - Hint: use pandas' [`dt` attribute](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.dt.html) to access easily datetimelike properties.
    # - Specify labels for these columns and set to `column_labels`
    # - Create a dataframe, `time_df,` containing the time data for this file by combining `column_labels` and `time_data` into a dictionary and converting this into a dataframe

    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame()

    for i, row in log_df.iterrows():
        timestamp = pd.Series(pd.to_datetime(row['ts'], unit='ms'))
        time_data = (timestamp.values[0], 
                timestamp.dt.hour.values[0], 
                timestamp.dt.day.values[0], 
                timestamp.dt.week.values[0], 
                timestamp.dt.month.values[0], 
                timestamp.dt.year.values[0],
                timestamp.dt.weekday.values[0])
        
        time_df = time_df.append(pd.DataFrame(data = [time_data], columns = column_labels), ignore_index = True)
            
    time_df.head()

    # #### Insert Records into Time Table
    # Implement the `time_table_insert` query in `sql_queries.py` and run the cell below to insert records for the timestamps in this log file into the `time` table. Remember to run `create_tables.py` before running the cell below to ensure you've created/resetted the `time` table in the sparkify database.

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        conn.commit()

    # ## #4: `users` Table
    # #### Extract Data for Users Table
    # - Select columns for user ID, first name, last name, gender and level and set to `user_df`

    user_df = log_df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # #### Insert Records into Users Table
    # Implement the `user_table_insert` query in `sql_queries.py` and run the cell below to insert records for the users in this log file into the `users` table. Remember to run `create_tables.py` before running the cell below to ensure you've created/resetted the `users` table in the sparkify database.

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        conn.commit()

    # ## #5: `songplays` Table
    # #### Extract Data and Songplays Table
    # This one is a little more complicated since information from the songs table, artists table, and original log file are all needed for the `songplays` table. Since the log file does not specify an ID for either the song or the artist, you'll need to get the song ID and artist ID by querying the songs and artists tables to find matches based on song title, artist name, and song duration time.
    # - Implement the `song_select` query in `sql_queries.py` to find the song ID and artist ID based on the title, artist name, and duration of a song.
    # - Select the timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent and set to `songplay_data`
    # 
    # #### Insert Records into Songplays Table
    # - Implement the `songplay_table_insert` query and run the cell below to insert records for the songplay actions in this log file into the `songplays` table. Remember to run `create_tables.py` before running the cell below to ensure you've created/resetted the `songplays` table in the sparkify database.

    column_labels = ('user_id', 'song_id', 'artist_id', 'start_time', 'session_id', 'level', 'location', 'user_agent')
    songplay_df = pd.DataFrame()

    for index, row in log_df.iterrows():
        # get songid and artistid from song and artist tables
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        song_id, artist_id = results if results else None, None

        # insert songplay record
        songplay_data = ( # songplay_id, user_id, song_id, artist_id, start_time, session_id, level, location, user_agent
            row.userId, 
            song_id, 
            artist_id, 
            pd.Timestamp(row.ts, unit='ms'),
            row.sessionId,
            row.level,
            row.location,
            row.userAgent)
        
        songplay_df = songplay_df.append(pd.DataFrame(data = [songplay_data], columns = column_labels), ignore_index = True)

    for i, row in songplay_df.iterrows():
        cur.execute(songplay_table_insert, list(row))
        conn.commit()


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    print("Processing song data...")
    process_song_data(cur, conn, filepath='data/song_data')

    print("Processing log data...")
    process_log_data(cur, conn, filepath='data/log_data')

    print("ETL process complete. Closing connection...")
    #Close Connection to Sparkify Database
    conn.close()


if __name__ == "__main__":
    main()