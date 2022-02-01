"""
This module accommodates to wrap instantiated log4j object using SparkContext
Log4j logging for use of PySpark.

"""
from config.spark_config import SparkProvider


class Logger(object):

    def __init__(self):
        # Prefix Log Message

        spark_provider = SparkProvider()

        spark_config = spark_provider.spark_session.sparkContext.getConf()
        application_id = spark_config.get('spark.app.id')
        application_name = spark_config.get('spark.app.name')

        apache_log4j = spark_provider.spark_session._jvm.org.apache.log4j
        log_prefix = '<' + application_id + ' ' + application_name + '>'
        self.logger = apache_log4j.LogManager.getLogger(log_prefix)

    def error(self, message):
        """Log an error.

        :param: Error message to write to log
        :return: None
        """
        self.logger.error(message)
        return None

    def warn(self, message):
        """Log a warning.

        :param: Warning message to write to log
        :return: None
        """
        self.logger.warn(message)
        return None

    def info(self, message):
        """Log information.

        :param: Information message to write to log
        :return: None
        """
        self.logger.info(message)
        return None
