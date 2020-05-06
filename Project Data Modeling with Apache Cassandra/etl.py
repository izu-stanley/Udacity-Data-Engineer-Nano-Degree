import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cql_queries import *

def process_event_file(session, filepath):
    """
    Performs ETL on one file and inserts data
    into the DB
    
    :params session: The connection to the DB
    :params filepath: The path to the file to be processed
    :return None
    """
    print('Starting Processing..')
    num_lines_1 = 0
    num_lines_2 = 0
    num_lines_3 = 0
    with open(filepath, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader)
        for line in csvreader:
            session.execute(song_in_session_insert, (int(line[8]),int(line[3]),line[0],line[9],float(line[5])))
            num_lines_1 += 1
        table_name = 'song_in_session'
        print('Processed lines: {} in {} table'.format(num_lines_1,table_name))
        print('Data inserted successfully into {} table'.format(table_name))

        
    with open(filepath, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) 
        for line in csvreader:
            session.execute(artist_in_session_insert,(int(line[10]),int(line[8]),line[0],line[9],int(line[3]), line[1],line[4]))
            num_lines_2 += 1
        table_name = 'artist_in_session'
        print('Processed lines: {} in {} table'.format(num_lines_2, table_name))
        print('Data inserted successfully into {} table'.format(table_name))
    
    
    with open(filepath, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) 
        for line in csvreader:
            session.execute(user_and_song_insert,(line[9],int(line[10]),line[1],line[4]))
            num_lines_3 += 1
        table_name = 'user_and_song'
        print('Processed lines: {} in {} table'.format(num_lines_3, table_name))
        print('Data inserted successfully into {} table'.format(table_name))
        
        
def process_data(session, filepath, target_file, func):
    """
    Goes through the entire data directory and combines input data into a new CSV file
    
    :params session: The connection to the DB
    :params filepath: Path to CSV files
    :params target_file: File name to which processed data is persisted
    :params func: Callback function to be called after processing is done
    :return None
    """

    file_path_list = []
    for root, dirs, files in os.walk(filepath):
        for f in files :
            file_path_list.append(os.path.abspath(f))
            print('Add to list: ' + f)
        num_files = len(file_path_list)
        print('{} files found in {}\n'.format(num_files, filepath))
        file_path_list = glob.glob(os.path.join(root,'*'))
    full_data_rows_list = []

    for f in file_path_list:
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader)
            for line in csvreader:
                full_data_rows_list.append(line)

    print('Total number of rows: ', len(full_data_rows_list))
    
    csv.register_dialect('myDialect',quoting=csv.QUOTE_ALL,skipinitialspace=True)

    with open(target_file, 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length','level','location','sessionId', 'song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13],row[16]))
        print('Input data files filtered successfully to: ' + target_file)

    func(session, target_file)
    print('All Data Processed and Inserted to DB.')

def main():
    """
    Connects to DB
    Calls helper function (process_data) to process input data
    :params None
    :return None
    """
    
    filepath = os.getcwd() + '/event_data'
    target_file = 'event_datafile_new.csv'
    
    from cassandra.cluster import Cluster
    try:
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect()
    except Exception as e:
        print(e)

    try:
        session.set_keyspace('sparkifydb')
    except Exception as e:
        print(e)

    process_data(session, filepath, target_file, func=process_event_file)
    
    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    main()
