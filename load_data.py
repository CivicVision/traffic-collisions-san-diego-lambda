import sys, os, json

here = os.path.dirname(os.path.realpath(__file__))
env_path = os.path.join(here, "./venv/lib/python2.7/site-packages/")
sys.path.append(env_path)

import agate
import agateremote
import googlemaps

gmaps = googlemaps.Client(key='AIzaSyASamO-yIbsV9Ml6ySteFK12XD2xbleTHU')

def load_2017_killed_data():
    specified_types = {
        'killed': agate.Number(),
        'injured': agate.Number(),
        'date_hour': agate.Text()
    }
    return agate.Table.from_url('https://s3.amazonaws.com/traffic-sd/accidents_killed_2017.csv', column_types=specified_types)


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

def format_address(d):
    street_address = "{} {}".format(d["street_name"], d["street_type"])
    if d["cross_st_name"]:
        return "{} and {} {}".format(street_address, d["cross_st_name"], d["cross_st_type"])
    else:
        if d["street_dir"]:
            return "{} {} {}".format(d["street_no"], d["street_dir"], street_address)
        else:
            return "{} {}".format(d["street_no"], street_address)

def gmaps_geocode(d):
    result = gmaps.geocode("{}, San Diego, CA, USA".format(format_address(d)))
    if len(result) > 0:
        return "{} {}".format(result[0]["geometry"]["location"]["lat"], result[0]["geometry"]["location"]["lng"])
    return ""

def geocode(table):
    return table.compute([
        ('location', agate.Formula(agate.Text(), gmaps_geocode))
    ])
