import agate
from decimal import Decimal

class StandardDeviations(agate.Computation):
    def __init__(self, column, deviations):
        self._st_dev_column = column
        self._deviations = Decimal(deviations)

    def get_computed_data_type(self, table):
        return agate.Boolean()

    def run(self, table):
        new_column = []
        st_dev = table.aggregate(agate.StDev(self._st_dev_column))
        mean = table.aggregate(agate.Mean(self._st_dev_column))

        deviations_range = range(mean-(self._deviations*st_dev),mean+(self._deviations*st_dev))

        for row in table.rows:
            val = row[self._st_dev_column]

            if val in deviations_range:
                new_column.append(True)
            else:
                new_column.append(False)

        return new_column
