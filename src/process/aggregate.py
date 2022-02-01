from pyspark.sql import Window
from pyspark.sql.functions import col,sum, count

from config.dataframe_config import DataframeProvider
from config.logger_config import Logger
from config.spark_config import SparkProvider

spark = SparkProvider().spark_session
log = Logger()
dfp = DataframeProvider()

def extract_data():
    hockey_df = spark.read \
        .option("header", True)\
        .csv("resources/Goalies.csv")
    dfp.hockey_df = hockey_df

    return hockey_df

def aggregate():
    hockey_df = extract_data()
    hockey_df.show(2)
    agg_partition = Window.partitionBy("tmID", "year")
    hockey_df = hockey_df.withColumn("Total_Wins", sum("W").over(agg_partition))\
        .withColumn("Total_Players", count("playerId").over(agg_partition))\
        .withColumn("Total_Losses", sum("L").over(agg_partition))\
        .withColumn("Total_Games_Played", sum("GP").over(agg_partition))\
        .withColumn("Total_Minutes_Played", sum("Min").over(agg_partition)) \
        .withColumn("Total_Goals_Against", sum("GA").over(agg_partition)) \
        .withColumn("Total_Shots_Against", sum("SA").over(agg_partition))

    hockey_df.show()
