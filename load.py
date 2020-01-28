from variables import *
import csv
import psycopg2
import sys

csv_path = 'Src/Polls/Focus/focus_data_20200126.csv'
conn = None
f = None

try:
    conn = psycopg2.connect(f"host={host} dbname=mandaty user=miso password={password}")
    cur = conn.cursor()

    f = open(csv_path, 'r')
    next(f) # Skip the header row.
    cur.execute('truncate table test')
    cur.copy_from(f, "test", sep=';')
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

    if f:
        f.close()