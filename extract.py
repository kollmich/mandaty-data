from variables import *
import csv
import psycopg2
import sys
from pandas import read_csv

csv_path = 'output/data_mandaty.csv'
csv_path2 = 'output/data_politicians.csv'

try:
    conn = psycopg2.connect(f"host={host} dbname=mandaty user=miso password={password}")
    cur = conn.cursor()

    fout = open(csv_path, 'w')
    cur.copy_to(fout, "visualisation", sep=',')
    #cur.execute(f'''COPY view_visualisation_2 TO STDOUT '{csv_path}' USING DELIMITERS ',' WITH CSV;''')

    df = read_csv(csv_path, sep=',')
    df.columns = ['result_id', 'poll_date', 'poll_agency', 'party_shortname', 'poll_result', 'moving_average']
    df.to_csv(csv_path, sep=',')

    df2 = read_csv(csv_path2, sep=',')
    df2.columns = ['poll_date', 'politician', 'party_id', 'approval', 'disapproval', 'agency_id', 'party_shortname']
    df2.to_csv(csv_path2, sep=',')

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

