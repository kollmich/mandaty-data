import csv
import psycopg2

csv_path = 'Src/Polls/Focus/focus_data_20200126.csv'

conn = psycopg2.connect("host=35.195.243.9 dbname=mandaty user=miso password=KbwZWxZ7TdJXJBpf")
cur = conn.cursor()

with open(csv_path, 'r') as f:
# Notice that we don't need the `csv` module.
    next(f) # Skip the header row.
    cur.copy_from(f, "test", sep=';')
    conn.commit()
