from variables import *
import csv
import psycopg2
import sys
from pandas import read_csv
import pandas as pd

csv_path_1 = 'output/data_mandaty.csv'
csv_path_2 = 'output/data_politicians.csv'
file_paths = [csv_path_1, csv_path_2]

input_db_1 = 'visualisation'
input_db_2 = 'popularity'
input_dbs = [input_db_1, input_db_2]

conn = psycopg2.connect(f"host={host} dbname=mandaty user=miso password={password}")
cur = conn.cursor()

for i,j in zip(range(0,len(file_paths)), range(0, len(input_dbs))):
    try:
        fout = open(file_paths[i], 'w')
        cur.copy_to(fout, input_dbs[j], sep=',')

    except psycopg2.DatabaseError as e:

        print(f'Error {e}')
        sys.exit(1)

    except IOError as e:

        print(f'Error {e}')
        sys.exit(1)

    finally:
        if fout:
            fout.close()

if conn:
    conn.close()

columns_mandaty = ['result_id', 'poll_date', 'poll_agency', 'party_shortname', 'poll_result', 'moving_average']
columns_popularity = ['poll_date', 'politician', 'party_id', 'approval', 'disapproval', 'agency_id', 'party_shortname']
columns_list = [columns_mandaty, columns_popularity]

for i,j in zip(range(0,len(file_paths)), range(0,len(columns_list))):
    df = read_csv(file_paths[i], sep=',', names=columns_list[j])
    df.to_csv(file_paths[i], sep=',', index=False)

df = read_csv(file_paths[0],sep=',')
relevant_parties = df[(df['poll_date'] == max(df['poll_date'])) & df['poll_result'] >= 0.02 ].party_shortname
df = df[df.party_shortname.isin(relevant_parties)]
df.to_csv(file_paths[0], sep=',', index=False)
