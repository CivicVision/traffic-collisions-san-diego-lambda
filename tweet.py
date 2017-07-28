import sys, os, json

here = os.path.dirname(os.path.realpath(__file__))
env_path = os.path.join(here, "./venv/lib/python2.7/site-packages/")
sys.path.append(env_path)

import tweepy
import agate
from datetime import datetime

def tweet_status(message):
    auth = tweepy.OAuthHandler(os.environ['consumer_token'], os.environ['consumer_secret'])
    auth.set_access_token(os.environ['access_token'], os.environ['access_secret'])
    print(auth)
    api = tweepy.API(auth)
    print(api.update_status(message))

def tweet_fatality(row):
    date = '{:%b %d, %I%p}'.format(row.get('date_time'))
    status = 'New traffic fatality has been reported on {}. The dashboard has been updated: https://civicvision.de/vision-zero-dashboard '.format(date)
    return tweet_status(status)
