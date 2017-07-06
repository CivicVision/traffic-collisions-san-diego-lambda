import sys, os, json

here = os.path.dirname(os.path.realpath(__file__))
env_path = os.path.join(here, "./venv/lib/python2.7/site-packages/")
sys.path.append(env_path)

import agate
from decimal import Decimal

class GroupRanking(agate.Computation):
    def __init__(self, column, group_column):
        self._group_column = group_column
        self._rank_column = column

    def get_computed_data_type(self, table):
        return agate.Number()

    def rating_value(self,row):
        return row[self._rank_column]

    def run(self, table):
        values = {}

        for row in table.rows:
            group = row[self._group_column]
            if group not in values:
                values[group] = []
            values[group].append(self.rating_value(row))

        ranks = {}
        for group in values:
            group_values = sorted(values[group])
            rank = 0

            if group not in ranks:
                ranks[group] = {}

            for c in group_values:
                rank += 1

                if c in ranks[group]:
                    rank -= 1
                    continue

                ranks[group][c] = Decimal(rank)

        new_column = []
        for row in table.rows:
            new_column.append(ranks[row[self._group_column]][self.rating_value(row)])

        return new_column

class RankWeightedAccidents(agate.Computation):
    def __init__(self, year_column):
        self._year_column = year_column

    def get_computed_data_type(self, table):
        return agate.Number()

    def rating_value(self,row):
        return (Decimal(0.5)*Decimal(row['accidents'])+Decimal(row['injured'])+Decimal(2)*Decimal(row['killed']))

    def run(self, table):
        values = {}

        for row in table.rows:
            year = row[self._year_column]
            if year not in values:
                values[year] = []
            values[year].append(self.rating_value(row))

        ranks = {}
        for year in values:
            year_values = sorted(values[year])
            rank = 0

            if year not in ranks:
                ranks[year] = {}

            for c in year_values:
                rank += 1

                if c in ranks[year]:
                    continue

                ranks[year][c] = Decimal(rank)

        new_column = []
        for row in table.rows:
            new_column.append(ranks[row[self._year_column]][self.rating_value(row)])

        return new_column

