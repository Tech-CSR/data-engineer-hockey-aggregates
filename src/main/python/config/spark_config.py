"""
This Module accommodates helper methods on initialize SparkSession (Entry point to SparkSQL) using PySpark API

"""

from pyspark.sql import SparkSession


class SparkProvider(object):

    def __init__(self):
        self._spark_session = SparkSession \
            .builder \
            .master('local[*]') \
            .appName('HockeyAggregate') \
            .getOrCreate()

    @property
    def spark_session(self):
        return self._spark_session
