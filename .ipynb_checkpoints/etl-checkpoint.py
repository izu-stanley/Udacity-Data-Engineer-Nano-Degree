import os
import glob
import psycopg2
import datetime
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    """
    Process song and artist data with a given insert_query
    :param cur: The cursor to execute queries
    :param filepath: the location of a single JSON song file on disk

    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id', 'year', 'duration']].values[0]
    song_data = song_data.tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = artist_data = df[['artist_id','artist_name','artist_location', 'artist_latitude', 'artist_longitude']].values[0]
    artist_data = artist_data.tolist()
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    """
    parse json file and extract relevant infor (time,user,songplay information) and save to db
    :param cur: The cursor to execute queries
    :param filepath: the location of a single JSON song file on disk
    """

    df = pd.read_json(filepath, lines=True)
    df = df[df["page"] == "NextSong"]
    
    time = pd.to_datetime(df["ts"],unit="ms")
    
    
    time_data = [time,time.dt.hour, time.dt.day, time.dt.week, time.dt.month, time.dt.year, time.dt.dayofweek]
    labels = ["timestamp", "hour", "day", "week of year", "month", "year", "weekday"]
    
    
    time_dataframe = pd.DataFrame(dict(zip(labels, time_data)))
    
    for i, row in time_dataframe.iterrows():
        cur.execute(time_table_insert, list(row))


    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for index, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results is True:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data = (datetime.datetime.fromtimestamp(row.ts/1000.0), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    """
    Get files and their path for each directory and pass the files to the appropriate function as needed while saving
    the relevant records into the db

    cur: db cursor
    conn: db connection
    filepath: the location of a single file on disk
    func: the function that will perform ETL on the data
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))


    file_no = len(all_files)
    print('{} files found in {}'.format(file_no, filepath))


    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, file_no))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()