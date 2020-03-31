from variables import *
import sys
import psycopg2
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import io

# Set number of seats in parliament
svk_seats = 150

# Connect to the Google spreadsheet
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('mandates-023705ca838d.json', scope)
client = gspread.authorize(creds)
worksheets = client.open('mandates-data').worksheets()

# Fetch party poll data
parties_sheet = client.open('mandates-data').worksheet('parties')
mandates = parties_sheet.get_all_records()
parties = pd.DataFrame.from_records(mandates)

# Add 5 days moving average, seats and quot columns
parties['mov_avg'] = parties.groupby('party_shortname')['result'].transform(lambda x: x.rolling(5, 1).mean())
parties['seats'] = 0
parties['quot'] = 0

# Split the parties according to the election thresholds (5% or 7% for a coalition)
winners = parties[((parties['coalition'] == 1) & (parties['result'] >= 0.07)) | ((parties['coalition'] == 0) & (parties['result'] >= 0.05))]
losers = parties[((parties['coalition'] == 1) & (parties['result'] < 0.07)) | ((parties['coalition'] == 0) & (parties['result'] < 0.05))]

# Create groups in which the seats need to be distributed and a temp table for calculated results
winners_grouped = winners.groupby(['poll_date','agency'])

# Define function to distribute each seat according to the d'Hondt method
def distribute_seats(group, total_seats):
    x = 0
    while x < total_seats:
        group.quot = group.result / (group.seats + 1)
        index = group.quot.idxmax(axis=0, skipna=True)
        group.seats[index] += 1
        x += 1

temp = pd.DataFrame(columns=['poll_date','agency', 'party_shortname', 'result', 'coalition', 'mov_avg', 'seats', 'quot'])

# Distribute seats in each group, add parties which didn't get to the parliament and sort
for group_name, group in winners_grouped:
    distribute_seats(group, svk_seats)
    temp = temp.append(group)

parties = temp.append(losers).drop('quot', axis=1).sort_values(by=['seats'], ascending=False).sort_values(by=['poll_date', 'agency'])

# Load to postgres db
try:
    conn = psycopg2.connect(f"host={host_aws} dbname={dbname_aws} user={user_aws} password={password_aws}")
    cur = conn.cursor()

    # parties buffer
    party_buf = io.StringIO()
    parties.to_csv(party_buf, sep='|',index=False, header=False)
    party_buf.seek(0)
    cur.execute('truncate table party_polls')
    cur.copy_from(party_buf, "party_polls", sep='|')
    conn.commit()

# Close connections
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

    if party_buf :
        party_buf.close()