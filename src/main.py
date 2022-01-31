# entry point for PySpark ETL application

from config.logger_config import Logger

from config.spark_config import SparkProvider

spark = SparkProvider()
log = Logger()


if __name__ == '__main__':
    log.warn("Spark Application Starting")
#Todo