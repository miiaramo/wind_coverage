import json
import datetime as dt
import pandas as pd
import sqlite3
from flask import Flask, render_template

import plotly
import plotly.graph_objs as go


app = Flask(__name__)
DB = 'finnish_wind_power_orig.db'
conn = None


@app.route('/')
def basic_view():
    with sqlite3.connect(DB) as conn:
        cursor = conn.cursor()

        df = pd.read_sql_query('select * from monthly_wind_coverage', conn)
        df['date'] = pd.to_datetime({'year':df['year'], 'month':df['month'], 'day':len(df['month'])*[1]})
        data = [go.Scatter(
            x=df['date'],
            y=df['percentage']
        )]
        monthly = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)

        df = pd.read_sql_query('select * from electricity_consumption', conn)
        df['month'] = pd.to_datetime(df['start_time']).dt.month
        df['year'] = pd.to_datetime(df['start_time']).dt.year
        df = df.drop(['index', 'start_time', 'end_time'], axis=1)
        df = df.groupby(['year', 'month'], as_index=False).mean()
        df['date'] = pd.to_datetime({'year':df['year'], 'month':df['month'], 'day':len(df['month'])*[1]})
        df = df.drop(['year', 'month'], axis=1)
        data = [go.Scatter(
            x=df['date'],
            y=df['value']
        )]
        consumption = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)

        df = pd.read_sql_query('select * from wind_power_generation', conn)
        df['month'] = pd.to_datetime(df['start_time']).dt.month
        df['year'] = pd.to_datetime(df['start_time']).dt.year
        df = df.drop(['index', 'start_time', 'end_time'], axis=1)
        df = df.groupby(['year', 'month'], as_index=False).mean()
        df['date'] = pd.to_datetime({'year':df['year'], 'month':df['month'], 'day':len(df['month'])*[1]})
        df = df.drop(['year', 'month'], axis=1)
        data = [go.Scatter(
            x=df['date'],
            y=df['value']
        )]
        generation = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)

        df = pd.read_sql_query('select * from hourly_wind_coverage', conn)
        df = df.drop(['month', 'day', 'hour'], axis=1)
        df = df[df['year'] < 2020]
        df = df.groupby(['year'], as_index=False).mean()
        df['date'] = pd.to_datetime({'year':df['year'], 'month':len(df['year'])*[12], 'day':len(df['year'])*[31]})
        data = [go.Scatter(
            x=df['date'],
            y=df['percentage']
        )]
        yearly = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)

    return render_template('index.html', monthly=monthly, generation=generation, consumption=consumption, yearly=yearly)


if __name__ == '__main__':
    app.run()