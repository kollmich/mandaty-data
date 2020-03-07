from variables import *
import sys
import psycopg2
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import io


# Connect to the Google spreadsheet
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('mandates-023705ca838d.json', scope)
client = gspread.authorize(creds)
worksheets = client.open('mandates-data').worksheets()

# Fetch popularity poll data
popularity_sheet = client.open('mandates-data').worksheet('popularity')
politicians = popularity_sheet.get_all_records()
popularity = pd.DataFrame.from_records(politicians)

# Load to postgres db
try:
    conn = psycopg2.connect(f"host={host_aws} dbname={dbname_aws} user={user_aws} password={password_aws}")
    cur = conn.cursor()

    # popularity buffer
    pop_buf = io.StringIO()
    popularity.to_csv(pop_buf, index=False, header=False)
    pop_buf.seek(0)
    cur.execute('truncate table popularity_polls')
    cur.copy_from(pop_buf, "popularity_polls", sep=',')
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

    if pop_buf:
        pop_buf.close()
