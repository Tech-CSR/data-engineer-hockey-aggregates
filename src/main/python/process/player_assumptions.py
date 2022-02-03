'''
This Module is to assume data points based on the stats
'''
from pyspark.sql import Window
from pyspark.sql.functions import from_json, col, max, min, count
from pyspark.sql.types import StructType, StructField, StringType

from config.logger_config import Logger

log = Logger()


def get_player_assumptions(final_hockey_df):
    '''

    :param final_hockey_df: Dataframe after requirements implemeted
    '''

    # Create Struct Schema with two dictionary columns names
    goals_stopped = StructType([
        StructField("PlayerId", StringType(), True),
        StructField("Goals_Stopped", StringType(), True)
    ])

    player_efficiency = StructType([
        StructField("PlayerId", StringType(), True),
        StructField("Efficiency", StringType(), True)
    ])

    # Split Dictionary to Columns
    # Most_Goals_Stopped playerID -> Most_Goals_Stopped_Player
    # Most_Efficient_Player playerID -> Most_Efficient_PlayerID
    most_goals_stopped = final_hockey_df.withColumn("jsonData", from_json(col("Most_Goals_Stopped"), goals_stopped)) \
        .select("tmID",
                "Year",
                "Wins_Agg",
                "Losses_Agg",
                "GP_Agg",
                "Mins_over_GA_agg",
                "GA_Over_SA_Agg",
                "Avg_Percentage_Wins",
                "Most_Goals_Stopped",
                "Most_Efficient_Player", "jsonData.*").withColumnRenamed("PlayerId", "Most_Goals_Stopped_Player").drop(
        "Goals_Stopped", "jsonData")

    most_efficient_player = most_goals_stopped.withColumn("jsonData",
                                                          from_json(col("Most_Efficient_Player"), player_efficiency)) \
        .select("tmID",
                "Year",
                "Wins_Agg",
                "Losses_Agg",
                "GP_Agg",
                "Mins_over_GA_agg",
                "GA_Over_SA_Agg",
                "Avg_Percentage_Wins",
                "Most_Goals_Stopped",
                "Most_Efficient_Player", "Most_Goals_Stopped_Player", "jsonData.*").withColumnRenamed("PlayerId",
                                                                                                      "Most_Efficient_PlayerID") \
        .drop(
        "Efficiency", "jsonData")

    # Filter out records to understand insights
    # Creating Dataframe with Most_Efficient_PlayerID same as Most_Goals_Stopped_Player
    # Creating Dataframe with Most_Efficient_PlayerID not same as Most_Goals_Stopped_Player

    same_efficient_player = most_efficient_player.filter(
        col("Most_Efficient_PlayerID") == col("Most_Goals_Stopped_Player"))
    diff_efficient_player = most_efficient_player.filter(
        col("Most_Efficient_PlayerID") != col("Most_Goals_Stopped_Player"))

    # Extracting performance of player with Most_Efficient_PlayerID same as Most_Goals_Stopped_Player
    same_efficient_player = same_efficient_player.groupBy("Most_Efficient_PlayerID").agg(
        max("Avg_Percentage_Wins").alias("Same_Max_Avg_Percentage_Wins"),
        min("Avg_Percentage_Wins").alias("Same_Min_Avg_Percentage_Wins"))

    # Extracting performance of player with Most_Efficient_PlayerID not same as Most_Goals_Stopped_Player
    diff_efficient_player = diff_efficient_player.groupBy("Most_Efficient_PlayerID").agg(
        max("Avg_Percentage_Wins").alias("Diff_Max_Avg_Percentage_Wins"),
        min("Avg_Percentage_Wins").alias("Diff_Min_Avg_Percentage_Wins"))

    final_player_assumption = same_efficient_player.join(diff_efficient_player,
                                                         same_efficient_player.Most_Efficient_PlayerID == diff_efficient_player.Most_Efficient_PlayerID) \
        .select(same_efficient_player["*"], diff_efficient_player["Diff_Max_Avg_Percentage_Wins"],
                diff_efficient_player["Diff_Min_Avg_Percentage_Wins"])

    # Viewing Performance of a  Common PlayerID against non-common PlayerID
    final_player_assumption.show(truncate=False)

    # Using PlayerID with differnt stats with both common and Non-Common
    # Filter data with Most_Efficient_PlayerID equal to winklha01
    # Filter data with Most_Goals_Stopped_Player equal to winklha01
    most_efficient_player.filter(col("Most_Efficient_PlayerID") == "winklha01").show(50, truncate=False)

    efficient_player = most_efficient_player.filter(col("Most_Goals_Stopped_Player") == "winklha01")
    efficient_player = efficient_player.withColumn("Cnt_Player_Associated",
                                                   count("Most_Goals_Stopped_Player").over(Window.partitionBy("tmID")))
    efficient_player.show(100, truncate=False)

    log.warn("Count of Teams where Most Efficient Player is Part of Most Goals scored --> " + str(
        efficient_player.select("tmID").distinct().count()))
