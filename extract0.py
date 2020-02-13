from variables import *
import csv
import psycopg2
import sys
from pandas import read_csv
import time

csv_path = 'output/data_politicians.csv'

try:
    conn = psycopg2.connect(f"host={host} dbname=mandaty user=miso password={password}")
    cur = conn.cursor()

    fout = open(csv_path, 'w')
    cur.copy_to(fout, "popularity", sep=',')
    # cur2.copy_expert("COPY popularity TO fout2 WITH CSV HEADER", fout2)
    #cur.execute(f'''COPY view_visualisation_2 TO STDOUT '{csv_path}' USING DELIMITERS ',' WITH CSV;''')

    # time.sleep(1)

    # df = read_csv('output/data_politicians.csv', sep=',')
    # df.columns = ['poll_date', 'politician', 'party_id', 'approval', 'disapproval', 'agency_id', 'party_shortname']
    # df.to_csv(csv_path, sep=',')

except psycopg2.DatabaseError as e:

    print(f'Error {e}')
    sys.exit(1)

except IOError as e:

    print(f'Error {e}')
    sys.exit(1)

finally:

    if conn:
        conn.close()

    if fout:
        fout.close()