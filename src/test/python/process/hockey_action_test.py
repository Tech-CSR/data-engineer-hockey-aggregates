'''
This Module contains Unit Tests to validate aggregation values calculated by hockey_action
'''

import unittest
from pyspark.sql.functions import col
from config.spark_config import SparkProvider
from process.hockey_action import extract_data, team_aggregate, player_win_percentage, player_performance, \
    transform_dataframe
from util.util import get_dictionary


class HockeyActionTest(unittest.TestCase):
    def setUp(self):
        self.spark = SparkProvider().spark_session
        self.agg_data = self.spark.read. \
            option("header", True) \
            .csv("test_data/agg_data.csv")

    def test_output_data(self):
        expected_df = self.spark.read. \
            option("header", True) \
            .csv("test_data/expected_data.csv")
        hockey_df = extract_data("test_data/Goalies.csv")
        hockey_agg_df = team_aggregate(hockey_df)
        hockey_prcnt = player_win_percentage(hockey_agg_df)
        hockey_performance = player_performance(hockey_prcnt)
        final_dataframe = transform_dataframe(hockey_performance)

        self.assertEqual(len(expected_df.columns), len(final_dataframe.columns))
        self.assertTrue([col in expected_df.columns for col in final_dataframe.columns])

    def test_team_aggregate(self):
        hockey_agg_df = team_aggregate(self.agg_data) \
            .filter(col("playerID") == 'wick1') \
            .select("tmID",
                    "year",
                    "Total_Wins",
                    "Total_Players",
                    "Total_Losses",
                    "Total_Games_Played",
                    "Total_Minutes_Played",
                    "Total_Goals_Against",
                    "Total_Shots_Against",
                    "Wins_Agg",
                    "Losses_Agg",
                    "GP_Agg",
                    "Mins_over_GA_agg",
                    "GA_Over_SA_Agg")

        final_data = get_dictionary(hockey_agg_df.filter(col("playerID") == 'wick1'), "tmID")

        expected_data = {"CAL": {
            "tmID": "CAL",
            "year": "2022",
            "Total_Wins": 37.0,
            "Total_Players": 2.0,
            "Total_Losses": 3.0,
            "Total_Games_Played": 41.0,
            "Total_Minutes_Played": 1357.0,
            "Total_Goals_Against": 51.0,
            "Total_Shots_Against": 276.0,
            "Wins_Agg": 18.5,
            "Losses_Agg": 1.5,
            "GP_Agg": 20.5,
            "Mins_over_GA_agg": 26.608,
            "GA_Over_SA_Agg": 0.185
        }
        }

        self.assertEqual(expected_data, final_data)

    def test_player_win_percentage(self):
        player_df = player_win_percentage(self.agg_data).filter(col("tmID") == 'HAR') \
            .select("playerID", "tmID", "year", "Player_Win_Prcnt", "Avg_Percentage_Wins")

        final_data = get_dictionary(player_df, "playerID")
        expected_data = {'boss1':
            {
                'playerID': 'boss1',
                'tmID': 'HAR',
                'year': '2010',
                'Player_Win_Prcnt': 60.0,
                'Avg_Percentage_Wins': 68.095
            },
            'boss2':
                {
                    'playerID': 'boss2',
                    'tmID': 'HAR',
                    'year': '2010',
                    'Player_Win_Prcnt': 76.19,
                    'Avg_Percentage_Wins': 68.095
                }
        }

        self.assertEqual(expected_data, final_data)

    def test_player_performance(self):
        performance_df = player_performance(self.agg_data) \
            .filter(col("tmID") == 'CAL') \
            .select("tmID", "Most_Goals_Stopped", "Most_Efficient_Player")

        final_data = get_dictionary(performance_df, "tmID")

        expected_data = {'CAL':
            {
                'tmID': 'CAL',
                'Most_Goals_Stopped': '{"PlayerId":"wick1","Goals_Stopped":33.0}',
                'Most_Efficient_Player': '{"PlayerId":"wick2","Efficiency":0.04309}'
            }
        }

        self.assertEqual(expected_data, final_data)


if __name__ == '__main__':
    unittest.main()
