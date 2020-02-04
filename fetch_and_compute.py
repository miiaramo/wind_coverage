import requests
import json
import datetime as dt
import pandas as pd
import sqlite3
from tokens import API_TOKEN

DB = 'finnish_wind_power_orig.db'
conn = None


def fetch_data():
    # Enter time as (year, month, day)
    start_time = dt.datetime(2015,1,1)
    end_time = dt.datetime(2020,1,31)

    # Find out how to format date to the following format: '2019-05-06T10:15:27Z'
    start_time_formatted = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time_formatted = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')

    token = TOKEN
    headers = {'x-api-key': '{}'.format(token)}

    # URL for wind
    fingrid_url = 'https://api.fingrid.fi/v1/variable/75/events/json?start_time={}&end_time={}'.format(start_time_formatted, end_time_formatted)
    response = requests.get(fingrid_url, headers=headers)
    wind_data = response.json()
    wind_df = pd.DataFrame(wind_data)

    # URL for consumption
    fingrid_url = 'https://api.fingrid.fi/v1/variable/124/events/json?start_time={}&end_time={}'.format(start_time_formatted, end_time_formatted)
    response = requests.get(fingrid_url, headers=headers)
    consumption_data = response.json()
    consumption_df = pd.DataFrame(consumption_data)

    DB = 'finnish_wind_power_orig.db'

    conn = None
    result = ''
        
    with sqlite3.connect(DB) as conn:
        cursor = conn.cursor()
        wind_df.to_sql('wind_power_generation', conn, if_exists='replace')
        consumption_df.to_sql('electricity_consumption', conn, if_exists='replace')


def compute_wind_coverages():
    with sqlite3.connect(DB) as conn:
        cursor = conn.cursor()
        w_df = pd.read_sql_query('select * from wind_power_generation;', conn).drop(['index'], axis=1)
        c_df = pd.read_sql_query('select * from electricity_consumption;', conn).drop(['index'], axis=1)
        
        all_df = pd.merge(w_df, c_df, on=['start_time','end_time'], how='inner')
        
        columns = ['year', 'month', 'day', 'hour', 'percentage']
        h_df = pd.DataFrame(columns=columns)
        h_df['year'] = pd.to_datetime(all_df['start_time']).dt.year
        h_df['month'] = pd.to_datetime(all_df['start_time']).dt.month
        h_df['day'] = pd.to_datetime(all_df['start_time']).dt.day
        h_df['hour'] = pd.to_datetime(all_df['start_time']).dt.hour
        h_df['percentage'] = all_df['value_x']/all_df['value_y']*100
        
        h_df.to_sql('hourly_wind_coverage', conn, if_exists='replace')

        d_df = h_df.drop('hour', axis=1)
        d_df = d_df.groupby(['year', 'month', 'day'], as_index=False).mean()

        d_df.to_sql('daily_wind_coverage', conn, if_exists='replace')

        m_df = d_df.drop('day', axis=1)
        m_df = m_df.groupby(['year', 'month'], as_index=False).mean()

        m_df.to_sql('monthly_wind_coverage', conn, if_exists='replace')
        m_df = d_df.drop('day', axis=1)
        m_df = m_df.groupby(['year', 'month'], as_index=False).mean()

        m_df.to_sql('monthly_wind_coverage', conn, if_exists='replace')


if __name__ == '__main__':
    fetch_data()
    compute_wind_coverages()
    print("Data available")

