from variables import *
import csv
import psycopg2
import sys
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import io

# connect to the Google spreadsheet
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('mandates-023705ca838d.json', scope)
client = gspread.authorize(creds)
worksheets = client.open('mandates-data').worksheets()

# fetch party poll data
parties_sheet = client.open('mandates-data').worksheet('parties')
mandates = parties_sheet.get_all_records()
parties = pd.DataFrame.from_records(mandates)
parties['mov_avg'] = parties.groupby('party_shortname')['result'].transform(lambda x: x.rolling(5, 1).mean())
subset = parties[(parties['poll_date'] == parties['poll_date'].max()) & (((parties['coalition'] == 1) & (parties['result'] >= 0.07)) | ((parties['coalition'] == 0) & (parties['result'] >= 0.05)))]
# print(parties)
# print(subset)
# print(subset['result'].transform(lambda x: x*10000).astype('int64'))

print(subset)

total_seats = 150

def dhondt_formula(votes, seats):
    return votes / (seats + 1)

dist = subset[['party_shortname','result']]
dist['mandates'] = 0
print(dist)
print(dist[dist['result'] == dist['result'].max()])

for i in range(0, total_seats):
    dist['quot'] = dist['result'] / (dist['mandates'] + 1)
    index = dist['quot'].idxmax(axis=0, skipna=True)
    dist['mandates'][index] += 1
    # dist['result'][index] = dist['result'][index] / (dist['mandates'][index] + 1)
    # print(dist['result'][index])
    print(dist)


# fetch popularity poll data
popularity_sheet = client.open('mandates-data').worksheet('popularity')
politicians = popularity_sheet.get_all_records()
popularity = pd.DataFrame.from_records(politicians)


# load to postgres db
try:
    conn = psycopg2.connect(f"host={host_aws} dbname={dbname_aws} user={user_aws} password={password_aws}")
    cur = conn.cursor()

    # parties buffer
    party_buf = io.StringIO()
    parties.to_csv(party_buf, index=False, header=False)
    party_buf.seek(0)
    cur.execute('truncate table party_polls')
    cur.copy_from(party_buf, "party_polls", sep=',')
    conn.commit()

    # popularity buffer
    pop_buf = io.StringIO()
    popularity.to_csv(pop_buf, index=False, header=False)
    pop_buf.seek(0)
    cur.execute('truncate table popularity_polls')
    cur.copy_from(pop_buf, "popularity_polls", sep=',')
    conn.commit()

except psycopg2.DatabaseError as e:

    if conn:
        conn.rollback()

    print(f'Error {e}')
    sys.exit(1)

except IOError as e:

    if conn:
        conn.rollback()

    print(f'Error {e}')
    sys.exit(1)

finally:

    if conn:
        conn.close()

    if pop_buf:
        pop_buf.close()

    if party_buf :
        party_buf.close()