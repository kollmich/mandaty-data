from variables import *
import sys
import csv
import psycopg2
import sys
from pandas import read_csv
import pandas as pd

# Define output files
csv_path_1 = 'output/data_mandaty.csv'
csv_path_2 = 'output/data_politicians.csv'
file_paths = [csv_path_1, csv_path_2]

input_db_1 = 'party_polls'
input_db_2 = 'popularity_polls'
input_dbs = [input_db_1, input_db_2]

# Define AWS connections
conn = psycopg2.connect(f"host={host_aws} dbname={dbname_aws} user={user_aws} password={password_aws}")
cur = conn.cursor()

# Connect to AWS
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

columns_mandaty = ['poll_date', 'poll_agency', 'party_shortname', 'result', 'coalition', 'mov_avg', 'seats']
columns_popularity = ['poll_date', 'politician', 'approval', 'disapproval', 'party_shortname']
columns_list = [columns_mandaty, columns_popularity]

for i,j in zip(range(0,len(file_paths)), range(0,len(columns_list))):
    df = read_csv(file_paths[i], sep=',', names=columns_list[j])
    df.to_csv(file_paths[i], sep=',', index=False)

# Write data to csv
df = read_csv(file_paths[0],sep=',')
relevant_parties = df[(df['poll_date'] == max(df['poll_date'])) & df['result'] >= 0.02 ].party_shortname

df = df[df.party_shortname.isin(relevant_parties)]
df['result'] = df['result'].apply(lambda x: round(x, 3))
df['mov_avg'] = df['mov_avg'].apply(lambda x: round(x, 3))
df.to_csv(file_paths[0], sep=',', index=False)
