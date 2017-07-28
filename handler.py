import sys, os, json

here = os.path.dirname(os.path.realpath(__file__))
env_path = os.path.join(here, "./venv/lib/python2.7/site-packages/")
sys.path.append(env_path)

import analysis
import load_data
import upload
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

    groupped_data = analysis.year_sum_counts(year_data)
    year_police_beat_data = analysis.year_police_beat_sum_counts(year_data)
    full_hour_data = analysis.sum_counts_by_full_hour(hour_data)
    overall_hour_data_analysis = analysis.sum_counts_by_hour(overall_hour_data)

    upload.killed_injured_year_police_beat(year_police_beat_data)
    upload.accidents(year_data)
    upload.killed_injured_year(groupped_data)
    upload.full_hour(full_hour_data)
    upload.hour_data(overall_hour_data_analysis)

    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response
