import agate
import agateremote
import googlemaps
import datetime
import os

import metadata

gmaps = googlemaps.Client(key=os.environ['GOOGLE_MAPS_KEY'])
traffic_source = 'http://seshat.datasd.org/pd/pd_collisions_datasd_v1.csv'

def load_year_killed_data(year):
    specified_types = {
        'killed': agate.Number(),
        'injured': agate.Number(),
        'date_hour': agate.Text()
    }
    return agate.Table.from_url('https://s3.amazonaws.com/traffic-sd/accidents_killed_{}.csv'.format(year), column_types=specified_types)


def load_last_fatality():
    return agate.Table.from_url('https://s3.amazonaws.com/traffic-sd/last_fatality.csv')

def load_last_collision():
    return agate.Table.from_url('https://s3.amazonaws.com/traffic-sd/last_collision.csv')

def load_data(data):
    data['table'] = agate.Table.from_url(traffic_source)
    return data

def load_all_data_by_year(year):
    data = {}
    data = load_data(data)
    data = add_year_column(data)
    return data['table'].where(lambda r: r['year'] == str(year))


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
    street_address = "{} {}".format(d[metadata.STREET_NAME], d[metadata.STREET_TYPE])
    if d[metadata.CROSS_STREET]:
        return "{} and {} {}".format(street_address, d[metadata.CROSS_STREET], d[metadata.CROSS_TYPE])
    else:
        if d[metadata.STREET_DIR]:
            return "{} {} {}".format(d[metadata.STREET_NO], d[metadata.STREET_DIR], street_address)
        else:
            return "{} {}".format(d[metadata.STREET_NO], street_address)

def gmaps_geocode(d):
    result = gmaps.geocode("{}, San Diego, CA, USA".format(format_address(d)))
    if len(result) > 0:
        return "{} {}".format(result[0]["geometry"]["location"]["lat"], result[0]["geometry"]["location"]["lng"])
    return ""

def get_last_report_id(data):
    last = data.order_by('date_time', reverse=True).limit(1)
    return last.rows[0].get('report_id')

def get_new_last_collision_report_id():
    table = load_all_data_by_year(datetime.date.today().year)
    return get_last_report_id(table)

def get_last_collision_report_id():
    last = load_last_collision()
    if len(last.rows) > 0:
        return get_last_report_id(last)

def get_new_last_fatality_report_id():
    last_year = datetime.date.today().year
    table = load_year_killed_data(last_year)
    return get_last_report_id(table)

def get_last_fatality_report_id():
    last = load_last_fatality()
    return get_last_report_id(last)

def last_report_values(data):
    last = data.order_by('date_time', reverse=True).limit(1)
    return last.rows.values()[0]

def get_last_killed_report_values():
    table = load_year_killed_data(datetime.date.today().year)
    return last_report_values(table)

def get_last_report_values():
    table = load_all_data_by_year(datetime.date.today().year)
    return last_report_values(table)

def geocode(table):
    return table.compute([
        ('location', agate.Formula(agate.Text(), gmaps_geocode))
    ])
