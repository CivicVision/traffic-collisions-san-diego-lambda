import sys, os, json

here = os.path.dirname(os.path.realpath(__file__))
env_path = os.path.join(here, "./venv/lib/python2.7/site-packages/")
sys.path.append(env_path)

import agate
import agateremote

def load_last_fatality():
    return agate.Table.from_url('https://s3.amazonaws.com/traffic-sd/last_fatality.csv')

def load_data(data):
    data['table'] = agate.Table.from_url('http://seshat.datasd.org/pd/pd_collisions_datasd.csv')
    return data

def load_police_beats(data):
    data['police_beats'] = agate.Table.from_url("http://seshat.datasd.org/pd/pd_beat_neighborhoods_datasd.csv")
    return data

def add_hour_column(data):
    data['table'] = data['table'].compute([
        ('hour', agate.Formula(agate.Text(), lambda r: r['date_time'].strftime("%H"))),
        ])
    return data

def add_year_column(data):
    data['table'] = data['table'].compute([
        ('year', agate.Formula(agate.Text(), lambda r: r['date_time'].strftime("%Y"))),
        ])
    return data

def add_full_hour_date(data):
    data['table'] = data['table'].compute([
        ('date_hour', agate.Formula(agate.Text(), lambda r: r['date_time'].strftime("%Y-%m-%d %H:00:00"))),
        ])
    return data

