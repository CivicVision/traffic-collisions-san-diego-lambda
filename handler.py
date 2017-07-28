import sys, os, json

here = os.path.dirname(os.path.realpath(__file__))
env_path = os.path.join(here, "./venv/lib/python2.7/site-packages/")
sys.path.append(env_path)

import analysis
import load_data
import upload
import tweet

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

def tweet_fatality(event, context):
    data = {}
    data['table'] = load_data.load_2017_killed_data()
    data['last'] = load_data.load_last_fatality()

    new_last = data['table'].order_by('date_time', reverse=True).limit(1)
    old_last = data['last'].order_by('date_time', reverse=True).limit(1)
    new_report_id = new_last_row.get('report_id')
    old_report_id = old_last_row.get('report_id')
    if old_report_id is not new_report_id:
        tweet.tweet_fatality(new_last.rows.values()[0])
        upload.upload_table(new_last, 'last_fatality.csv')
    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

