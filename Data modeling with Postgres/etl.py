import os
import glob
import psycopg2
import numpy as np
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    """process song_data dataset & insert single row values 
       into table 'songs' & 'artists'
    
    Parameters:
    -----------
    cur: db cursor object to execute queries
    filepath: path of the .json files of which 
              data to be inserted into tables
    """
    
    # open and read song dataset file
    df = pd.read_json(filepath, lines=True)

    #-------songs table-------#
    
    # extract data from dataFrame with column_names
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]]
    
    #----- Multi row insertion------#
    # 1. Write DataFrame into csv file
    temp_file_path = '/Users/manish/Documents/from_25Oct/data engineering/final_project/data/song_data/song_data.csv'
    song_data.to_csv(temp_file_path, index=False, header=False)# 
    
    # 2. Create Temporary table
    cur.execute(temp_songs_table_create)
    
    # 3. Insert data into temporary table
    cur.execute(song_temp_table_insert_)
    
    # 4. insert data into songs table
    cur.execute(song_table_insert)
    
    # 5. Delete temp file and temp table
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)
    else:
        print("The file does not exist")
    
    cur.execute("Drop Table songs_temp;")
    
    #------- artists table -------#
    
    #extract data into dataframe
    artists_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]]
    
    #----- Multi row insertion------#
    
    # 1. Write DataFrame into csv file
    temp_file_path = '/Users/manish/Documents/from_25Oct/data engineering/final_project/data/song_data/artists_data.csv'
    artists_data.to_csv(temp_file_path, index=False, header=False)
    
    # 2. Create Temporary table
    cur.execute(temp_artists_table_create)
    
    # 3. Insert data into temporary table
    cur.execute(artists_temp_table_insert_)
    
    # 4. insert data into artists table
    cur.execute(artist_table_insert)
    
    # 5. Delete temp file and Drop temp table
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)
    else:
        print("The file does not exist")
        
    cur.execute("Drop Table artists_temp;")

def process_log_file(cur, filepath):
    """process log_data dataset & insert single row values 
       into table 'time' & 'user'
    
    Parameters:
    -----------
    cur: db cursor object to execute queries
    filepath: path of the .json files of which 
              data to be inserted into tables
    """
    
    # open and read log_data dataset file
    df = pd.read_json(filepath, lines=True)
    
    # Filter records by NextSong action
    df_ = df[df['page'] == 'NextSong']

    #--------"time" table ---------#
    
    # Convert the ts timestamp column to datetime
    t = pd.to_datetime(df_["ts"], unit = "ms")
    timestamp_ = list(df_["ts"])
    hour_ = list(t.dt.hour)
    day_ = list(t.dt.day) 
    week_ = list(t.dt.week) 
    month_ = list(t.dt.month) 
    year_ = list(t.dt.year)
    weekday_ = list(t.dt.weekday) 
    time_data = [timestamp_, hour_, day_, week_, month_, year_, weekday_]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'] 
    
    #create time DataFrame via dictionary
    time_dict = {label :time_data[i]  for i, label in enumerate(column_labels)}
    time_df = pd.DataFrame(time_dict)
    
    # insert time data records
    #----- Multi row insertion------# 
    
    temp_file_path = '/Users/manish/Documents/from_25Oct/data engineering/final_project/data/log_data/time_data.csv'
    
    # 1. Write DataFrame into csv file
    time_df.to_csv(temp_file_path, index=False, header=False)
    
    # 2. Create Temporary table
    cur.execute(temp_time_table_create)
    
    # 3. insert data into Temporary table
    cur.execute(time_temp_table_insert)
    
    # 4. insert data into time table
    cur.execute(time_table_insert)
    
    # 5. Delete temp file and temp table
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)
    else:
        print("The file does not exist")
        
    cur.execute("Drop Table time_temp;")
    
    #-------users table---------#
    
    # Select columns for user ID, first name, last name, gender and level
    user_df_cols = ["userId", "firstName", "lastName", "gender", "level"]
    user_df = df_.loc[:, user_df_cols]
    
    # ---- multi row insertion
    # 1. Write DataFrame into csv file
    temp_file_path = '/Users/manish/Documents/from_25Oct/data engineering/final_project/data/log_data/users_data.csv'
    user_df.to_csv(temp_file_path, index=False, header=False)
    
    # 2. Create temp table
    cur.execute(temp_users_table_create)
    
    # 3. insert into users_temp table
    cur.execute(users_temp_table_insert)
    
    # 4. insert into users table
    cur.execute(user_table_insert)
    
    # 5. Delete temp file and temp table
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)
    else:
        print("The file does not exist")
        
    cur.execute("Drop Table users_temp;")
    
    #--------songplays table--------#
    
    for index, row in df_.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            
        # insert songplay record
        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as E:
            print("Error in inserting data into songplays table")
            print(e)


def process_data(cur, conn, filepath, func):
    """iterate over all files in a path and execute data loading 
    via SQL insert (as per parameter func)
    
    Parameters:
    -----------
    cur: db cursor object to execute queries
    conn: db connection object 
    filepath: path of the .json files of which 
              data to be inserted into tables
    func: function for data loading into the Tables
    """
    
    # get all files matching extension from directory 
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    make db connection and runs defined functions to process log files
    & to insert data into all tables
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=manish")
    cur = conn.cursor()

    process_data(cur, conn, filepath='/Users/manish/Documents/from_25Oct/data engineering/final_project/data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='/Users/manish/Documents/from_25Oct/data engineering/final_project/data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()