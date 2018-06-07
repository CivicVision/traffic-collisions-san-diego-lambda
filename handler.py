import sys, os, json

here = os.path.dirname(os.path.realpath(__file__))
env_path = os.path.join(here, "./venv/lib/python2.7/site-packages/")
sys.path.append(env_path)

import analysis
import filters
import load_data
import upload
import tweet

def geocode_collissions(event, context):
    data = {}
    data['table'] = load_data.load_2017_killed_data()

    data['data_2017'] = load_data.geocode(data['table'])

    upload.upload_file(data, 'data_2017', 'accidents_killed_2017_geocoded.csv')

    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

def traffic_collissions(event, context):
    data = {}
    data = load_data.load_data(data)
    year_data = load_data.add_year_column(data)
    hour_data = load_data.add_full_hour_date(data)
    overall_hour_data = load_data.add_hour_column(data)

    data['2017'] = analysis.data_for_year(data['table'], '2017')
    data['killed_2017'] = analysis.filter_table_func(data['2017'], filters.killed)

    charge_2017 = analysis.sum_counts_group(data['2017'], 'charge_desc')
    charge_year = analysis.sum_counts_group(data['table'].group_by('year'), 'charge_desc')
    charge = analysis.sum_counts_group(data['table'], 'charge_desc')

    street_name_2017 = analysis.sum_counts_group(data['2017'],'street_name')
    street_name_year = analysis.sum_counts_group(data['table'].group_by('year'),'street_name')
    street_name = analysis.sum_counts_group(data['table'],'street_name')

    groupped_data = analysis.year_sum_counts(year_data)
    year_police_beat_data = analysis.year_police_beat_sum_counts(year_data)
    full_hour_data = analysis.sum_counts_by_full_hour(hour_data)
    overall_hour_data_analysis = analysis.sum_counts_by_hour(overall_hour_data)

    upload.killed_injured_year_police_beat(year_police_beat_data)
    upload.accidents(year_data)
    upload.killed_injured_year(groupped_data)
    upload.full_hour(full_hour_data)
    upload.hour_data(overall_hour_data_analysis)

    upload.upload_file(data, 'killed_2017', 'accidents_killed_2017.csv')
    upload.upload_file(data, '2017', 'accidents_2017.csv')

    upload.upload_table(charge_year, 'year_charge_desc.csv')
    upload.upload_table(charge_2017, 'charge_desc_2017.csv')
    upload.upload_table(charge, 'charge_desc.csv')
    upload.upload_table(street_name_year, 'year_street_name.csv')
    upload.upload_table(street_name_2017, 'street_name_2017.csv')
    upload.upload_table(street_name, 'street_name.csv')

    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

def tweet_fatality(event, context):
    result = tweet.tweet_last_fatality()
    response = {
        "statusCode": 200,
        "body": json.dumps(result)
    }

    return response

