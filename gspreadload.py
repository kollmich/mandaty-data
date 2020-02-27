import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('mandates-023705ca838d.json', scope)
client = gspread.authorize(creds)
worksheets = client.open('mandates-data').worksheets()
print(worksheets)

parties_sheet = client.open('mandates-data').worksheet('parties')

mandates = parties_sheet.get_all_records()

parties = pd.DataFrame.from_records(mandates)

print(parties)


popularity_sheet = client.open('mandates-data').worksheet('popularity')

politicians = popularity_sheet.get_all_records()

popularity = pd.DataFrame.from_records(politicians)

print(popularity)