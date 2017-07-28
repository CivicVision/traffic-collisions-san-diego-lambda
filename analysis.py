import sys, os, json

here = os.path.dirname(os.path.realpath(__file__))
env_path = os.path.join(here, "./venv/lib/python2.7/site-packages/")
sys.path.append(env_path)

import agate
from ranking import GroupRanking, RankWeightedAccidents
count_accidents_injured = agate.Summary('injured', agate.Number(), lambda r: sum(injured > 0 for injured in r.values()))

def sum_counts_by_full_hour(data):
    data['full_hour']= data['table'].group_by('date_hour').aggregate([
        ('killed', agate.Sum('killed')),
        ('injured', agate.Sum('injured')),
        ('accidents', agate.Count())
    ])
    return data

def sum_counts_by_hour(data):
    data['hour']= data['table'].group_by('hour').aggregate([
        ('killed', agate.Sum('killed')),
        ('injured', agate.Sum('injured')),
        ('accidents', agate.Count()),
        ('accidents_injured', count_accidents_injured)
    ]).compute([
        ('killed_percent', agate.Percent('killed')),
        ('injured_percent', agate.Percent('injured')),
        ('accidents_percent', agate.Percent('accidents')),
    ]).compute([
        ('weighted', agate.Formula(agate.Number(), lambda r: r['killed_percent']+r['injured_percent']))
    ])
    return data

def statistics(data):
    data['statistics']= data['table'].aggregate([
        ('killed', agate.Sum('killed')),
        ('injured', agate.Sum('injured')),
        ('accidents', agate.Count()),
        ('mean_accidents', agate.Mean('accidents')),
        ('mean_killed', agate.Mean('killed')),
        ('mean_injured', agate.Mean('injured'))
    ])
    return data

def year_sum_counts(data):
    data['groupped_year']= data['table'].group_by('year').aggregate([
        ('killed', agate.Sum('killed')),
        ('injured', agate.Sum('injured')),
        ('accidents', agate.Count()),
        ('accidents_injured', count_accidents_injured)
    ])
    return data

def year_police_beat_sum_counts(data):
    data['year_police_beat']= data['table'].group_by('year').group_by('police_beat').aggregate([
        ('killed', agate.Sum('killed')),
        ('injured', agate.Sum('injured')),
        ('accidents', agate.Count())
        ]).compute([
            ('weighted_rank', RankWeightedAccidents('year')),
            ('killed_rank', GroupRanking('killed', 'year')),
            ('accidents_rank', GroupRanking('accidents', 'year')),
            ('injured_rank', GroupRanking('injured', 'year'))
        ])
    return data

