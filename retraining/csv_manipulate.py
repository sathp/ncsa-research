#Imports
import pandas
import geostreams_script
from datetime import timedelta

# File path to dataframe csv
leaf_des = '../bety-csv/leaf_dissecation_6000000270.csv'

# Initialize dataframe in pandas
df = pandas.read_csv(leaf_des, parse_dates=['date'])
df['start-date'] = pandas.to_datetime(df['date']).map(str)
df['end-date'] = pandas.to_datetime(df['date'] + timedelta(days=1)).map(str)

# print(df['only-date'])

# Filtering dataframe based on truth value
true = df.query('mean==1')
false = df.query('mean==0')

# Running geostreams script
for index, row in true.iterrows():
    # print(row['sitename'], row['only-date'].replace(' 05:00:00', ''))
    geostreams_script.fetch(row['sitename'], row['start-date'].replace(' 05:00:00', ''), row['end-date'])