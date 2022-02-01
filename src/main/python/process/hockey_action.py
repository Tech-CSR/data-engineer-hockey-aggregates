"""
This Module process Hockey Dataset as Spark Daraframe
Calculating Aggregations, Win Percentage and Player performance
"""

from pyspark.sql import Window
from pyspark.sql.functions import sum, count, col, lit, row_number, to_json, struct, round
from config.dataframe_config import DataframeProvider
from config.logger_config import Logger
from config.spark_config import SparkProvider

spark = SparkProvider().spark_session
log = Logger()
dfp = DataframeProvider()
agg_partition = Window.partitionBy("tmID", "year")


def extract_data(path):
    log.warn("Reading Goalies hockey dataset")

    try:
        hockey_df = spark.read \
            .option("header", True) \
            .csv(path)
    except ValueError as read_error:
        log.error(read_error)
        raise

    return hockey_df


def team_aggregate(hockey_df):
    log.warn("Calculating player stats for each tmID and Year")
    hockey_agg_df = hockey_df.withColumn("Total_Wins", sum("W").over(agg_partition)) \
        .withColumn("Total_Players", count("playerId").over(agg_partition)) \
        .withColumn("Total_Losses", sum("L").over(agg_partition)) \
        .withColumn("Total_Games_Played", sum("GP").over(agg_partition)) \
        .withColumn("Total_Minutes_Played", sum("Min").over(agg_partition)) \
        .withColumn("Total_Goals_Against", sum("GA").over(agg_partition)) \
        .withColumn("Total_Shots_Against", sum("SA").over(agg_partition))

    log.info("Calculating Hockey Aggregates")
    hockey_agg_df = hockey_agg_df.withColumn("Wins_Agg", col("Total_Wins") / col("Total_Players")) \
        .withColumn("Losses_Agg", col("Total_Losses") / col("Total_Players")) \
        .withColumn("GP_Agg", col("Total_Games_Played") / col("Total_Players")) \
        .withColumn("Mins_over_GA_agg", round(col("Total_Minutes_Played") / col("Total_Goals_Against"),3)) \
        .withColumn("GA_Over_SA_Agg", round(col("Total_Goals_Against") / col("Total_Shots_Against"),3))

    return hockey_agg_df


def player_win_percentage(hockey_agg_df):
    log.info("Calculate Hockey Win Percentage")
    player_prcnt = hockey_agg_df.withColumn("Player_Win_Prcnt", round((col("W") * lit(100)) / col("GP"),3)) \
        .withColumn("Avg_Percentage_Wins",
                    round(sum("Player_Win_Prcnt").over(agg_partition) / count("playerID").over(agg_partition),3))

    return player_prcnt


def player_performance(player_prcnt):
    log.info("Calculating Player Efficiency")
    player_prcnt = player_prcnt.withColumn("Goals_Stopped", sum("SHO").over(Window.partitionBy("playerID"))) \
        .withColumn("Minutes_Played", sum("Min").over(Window.partitionBy("playerId"))).cache()

    goals_stopped = player_prcnt.withColumn("ROW_NUM", row_number().over(Window.partitionBy("tmID")
                                                                         .orderBy(col("Goals_Stopped").desc()))) \
        .filter(col("ROW_NUM") == lit(1)) \
        .select("PlayerID", "Goals_Stopped", "tmID")
    goals_stopped = goals_stopped.withColumn("Most_Goals_Stopped", to_json(struct("PlayerId", "Goals_Stopped")))

    efficient_player = player_prcnt.withColumn("Efficiency",
                                               (col("Goals_Stopped") / col("Minutes_Played")).cast('Decimal(38,5)')) \
        .withColumn("ROW_NUM", row_number().over(Window.partitionBy("tmID").orderBy(col("Efficiency").desc()))) \
        .filter(col("ROW_NUM") == lit(1)) \
        .select("PlayerID", "Efficiency", "tmID")
    efficient_player = efficient_player.withColumn("Most_Efficient_Player", to_json(struct("PlayerId", "Efficiency")))

    player_performance = player_prcnt.join(goals_stopped, player_prcnt.tmID == goals_stopped.tmID, "leftouter") \
        .select(player_prcnt["*"], goals_stopped["Most_Goals_Stopped"])
    player_performance = player_performance.join(efficient_player,
                                                 player_performance.tmID == efficient_player.tmID, "leftouter") \
        .select(player_performance["*"], efficient_player["Most_Efficient_Player"])

    return player_performance


def transform_dataframe(hockey_agg):
    log.info("Performing final transformation")
    final_dataframe = hockey_agg.withColumn("ROW_NUM", row_number().over(agg_partition.orderBy("year"))) \
        .filter(col("ROW_NUM") == 1) \
        .select("tmID",
                "Year",
                "Wins_Agg",
                "Losses_Agg",
                "GP_Agg",
                "Mins_over_GA_agg",
                "GA_Over_SA_Agg",
                "Avg_Percentage_Wins",
                "Most_Goals_Stopped",
                "Most_Efficient_Player"
                )
    dfp.hockey_df = final_dataframe
    return final_dataframe