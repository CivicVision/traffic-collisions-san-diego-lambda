import sys, os, json

here = os.path.dirname(os.path.realpath(__file__))
env_path = os.path.join(here, "./venv/lib/python2.7/site-packages/")
sys.path.append(env_path)

import boto3

def upload_file_to_s3(bucket_file, local_file):
    session = boto3.session.Session()
    s3 = session.resource('s3')
    s3.Bucket('traffic-sd').put_object(Key=bucket_file, Body=open(local_file, 'rb'))

def upload_file(data, data_key, filename):
    tmp_filename = '/tmp/{}.csv'.format(data_key)
    data[data_key].to_csv(tmp_filename)
    upload_file_to_s3(filename,tmp_filename)

def killed_injured_year(data):
    upload_file(data,'groupped_year', 'accidents_killed_injured_b_year.csv')

def killed_injured_year_police_beat(data):
    upload_file(data,'year_police_beat', 'accidents_killed_injured_b_year_police_beat.csv')

def accidents(data):
    upload_file(data,'table', 'accidents.csv')

def full_hour(data):
    upload_file(data,'full_hour', 'full_hour_accidents.csv')

def hour_data(data):
    upload_file(data,'hour', 'per_hour_accidents.csv')
