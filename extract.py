from variables import *
import csv
import psycopg2
import sys

csv_path = 'output/data_mandaty.csv'

try:
    conn = psycopg2.connect(f"host={host} dbname=mandaty user=miso password={password}")
    cur = conn.cursor()

    fout = open(csv_path, 'w')
    cur.copy_to(fout, "visualisation", sep=',')
    #cur.execute(f'''COPY visualisation TO '{csv_path}' USING DELIMITERS ',' WITH CSV;''')

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