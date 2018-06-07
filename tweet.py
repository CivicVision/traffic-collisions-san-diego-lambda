import sys, os, json

here = os.path.dirname(os.path.realpath(__file__))
env_path = os.path.join(here, "./venv/lib/python2.7/site-packages/")
sys.path.append(env_path)

import tweepy
import agate
import load_data
import upload
from datetime import datetime

def tweet_last_fatality():
    if load_data.get_last_fatality_report_id() is not load_data.get_new_last_fatality_report_id():
        result = tweet_fatality(load_data.get_last_killed_report_values())
        upload.upload_table(last, 'last_fatality.csv')
    return result

def tweet_last_collision():
    if load_data.get_last_collision_report_id() is not load_data.get_new_last_collision_report_id():
        last = load_data.get_last_report_values()
        result = tweet_collision(last)
        upload.upload_table(last, 'last_collision.csv')
    return result

def tweet_status(message):
    return print(message)
    auth = tweepy.OAuthHandler(os.environ['consumer_token'], os.environ['consumer_secret'])
    auth.set_access_token(os.environ['access_token'], os.environ['access_secret'])
    api = tweepy.API(auth)
    print(api.update_status(message))

def tweet_collision(row):
    if row.get('fatality') and row.get('fatality') > 0:
        return tweet_fatality(row)
    elif row.get('injuries') and row.get('injuries') > 0:
        return tweet_injuriy_report(row)
    else:
        return tweet_collision_report(row)

def tweet_collision_report(row):
    date = '{:%b %d, %I%p}'.format(row.get('date_time'))
    status = 'New traffic collision has been reported on {}. The dashboard has been updated: https://civicvision.de/vision-zero-dashboard '.format(date)
    return tweet_status(status)

def tweet_injury_report(row):
    date = '{:%b %d, %I%p}'.format(row.get('date_time'))
    status = 'New traffic injury has been reported on {}. The dashboard has been updated: https://civicvision.de/vision-zero-dashboard '.format(date)
    return tweet_status(status)

def tweet_fatality(row):
    date = '{:%b %d, %I%p}'.format(row.get('date_time'))
    status = 'New traffic fatality has been reported on {}. The dashboard has been updated: https://civicvision.de/vision-zero-dashboard '.format(date)
    return tweet_status(status)

tweet_last_collision()
