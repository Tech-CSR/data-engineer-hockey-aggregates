# entry point for PySpark ETL application

from config.logger_config import Logger

from config.spark_config import SparkProvider
from process.hockey_action import team_aggregate, extract_data, player_win_percentage, player_performance, \
    transform_dataframe

spark = SparkProvider().spark_session
log = Logger()

if __name__ == '__main__':
    log.warn("Starting Spark Application")

    hockey_df = extract_data()
    hockey_agg_df = team_aggregate(hockey_df)
    hockey_prcnt = player_win_percentage(hockey_agg_df)
    hockey_performance = player_performance(hockey_prcnt)
    transform_dataframe(hockey_performance)