# entry point for PySpark ETL application

from config.logger_config import Logger

from config.spark_config import SparkProvider
from process.aggregate import aggregate

spark = SparkProvider().spark_session
log = Logger()

if __name__ == '__main__':
    log.warn("Starting Spark Application")
    aggregate()