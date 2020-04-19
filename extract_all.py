from variables import *
import sys
import csv
import psycopg2
import sys
from pandas import read_csv
import pandas as pd
###############
import io
import logging
import boto3
from botocore.exceptions import ClientError
import os


# Define output files
parties = io.StringIO()
politicians = io.StringIO()
file_paths = [parties, politicians]

input_db_1 = 'party_polls'
input_db_2 = 'popularity_polls'
input_dbs = [input_db_1, input_db_2]

# Define AWS connections
conn = psycopg2.connect(f"host={host_aws} dbname={dbname_aws} user={user_aws} password={password_aws}")
cur = conn.cursor()

# Connect to AWS
for i,j in zip(range(0,len(file_paths)), range(0, len(input_dbs))):
    try:
        cur.copy_to(file_paths[i], input_dbs[j], sep='|')
        file_paths[i].seek(0)

    except psycopg2.DatabaseError as e:

        print(f'Error {e}')
        sys.exit(1)

    except IOError as e:

        print(f'Error {e}')
        sys.exit(1)

if conn:
    conn.close()

columns_parties = ['poll_date', 'poll_agency', 'party_shortname', 'result', 'coalition', 'mov_avg', 'seats']
columns_politicians= ['poll_date', 'politician', 'approval', 'disapproval', 'party_shortname', 'birthdate', 'birthplace', 'occupation', 'bio', 'bio_en']
columns_list = [columns_parties, columns_politicians]

# Fetch and filter relevant parties data only
df = read_csv(file_paths[0],sep='|', names=columns_list[0])
relevant_parties = df[(df['poll_date'] == max(df['poll_date'])) & df['result'] >= 0.02 ].party_shortname
df = df[df.party_shortname.isin(relevant_parties)]
df['result'] = df['result'].apply(lambda x: round(x, 4))
df['mov_avg'] = df['mov_avg'].apply(lambda x: round(x, 4))
file_paths[0].truncate(0)
file_paths[0].seek(0)
df.to_csv(file_paths[0], sep='|', index=False)
file_paths[0].seek(0)

# Fetch and add column names for politicians data
df = read_csv(file_paths[1],sep='|', names=columns_list[1])
file_paths[1].truncate(0)
file_paths[1].seek(0)
df.to_csv(file_paths[1], sep='|', index=False)
file_paths[1].seek(0)

# S3 UPLOAD function
def upload_file(file_name, bucket, object_name=None):
    # Upload the file
    s3_client = boto3.resource('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    try:
        response = s3_client.Object(bucket, "2020/March/mandates/assets/data/{}.csv".format(file_name)).put(Body=object_name.getvalue())

    except ClientError as e:
        logging.error(e)
        return False
    return True

upload_file("data_mandaty","trendspotting.site",file_paths[0])
upload_file("data_politicians","trendspotting.site",file_paths[1])

