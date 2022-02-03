# Data Engineer Hockey Aggregates

## Introduction

This document is designed to elucidate the process of `data-engineer-hockey-aggregates` component. Technical details are highlighted to seamlessly run the project.

Following are addressed by this component 

- Read [Hockey Data Set](https://www.kaggle.com/open-source-sports/professional-hockey-database?select=Goalies.csv) dataset with Apache Spark 2.4 using pySpark API
- Perform Aggregates on dataframe
- Calculate overall most efficient placer for each hockey team

## Python Packages

- Python Version 3.7
- pySpark Version 2.4.0
- pySpark Stubs version 2.4.0

Apache Spark is chosen because of its intense speed in performing takes on large scale data and Spark is easy-to-use while operating with last datasets.
Using `Pyspark 2.4.0` API makes it very convenient way to implement Spark framework. `Python 3.7` is used as it is standard version recommended working with pySpark 2.4.0. Py Stubs are used to include accurate type hints.

**Note:** Python 3.8 or higher is not compatible with pyspark 2.4

## Execution Steps

### Managing Python Dependencies
Using pipenv for managing python dependencies. All python packages are cataloged in the Pipfile. The downstream dependencies are cataloged in Pipfile.lock

### Install Pipenv
Assuming Python 3.7 is installed in the system and PATH environmental variable.

Run below command from terminal:
```commandline
pip3 install pipenv
```
### Install Project Dependencies
Using Pycharm add PipEnv as python interpreter which automatically installs all dependencies. For more details checkout [PipEnv](https://www.jetbrains.com/help/pycharm/pipenv.html#pipenv-new-project)

Alternatively run `pipenv install --dev` command in terminal from project root or manually install packages and run with `python 3.7`

### Running `main.py` file
From project root terminal, redirect to `src/main/python` and run below command

```commandline
python main.py
```

### Running Test cases
From project root terminal, redirect to `src/test/python/process` and run below command
```commandline
python hockey_action_test.py

```


## Project Structure
```bash
root/
 |-- src/
 |   |-- main
 |   |-- | -- python
 |   |-- | -- | -- config
 |   |-- | -- | -- | -- dataframe_config.py
 |   |-- | -- | -- | -- logger_config.py
 |   |-- | -- | -- | -- spark_config.py
 |   |-- | -- | -- process
 |   |-- | -- | -- | -- hockey_action.py
 |   |-- | -- | -- resources
 |   |-- | -- | -- | -- Goalies.csv
 |   |-- | -- | -- util
 |   |-- | -- | -- | -- util.py
 |   |-- | -- | -- main.py
 |   |-- test
 |   |-- | -- python 
 |   |-- | -- | -- process
 |   |-- | -- | -- | -- hockey_action_test.py
 |   |-- | -- | -- test_data
 |   |-- | -- | -- | -- agg_data.csv
 |   |-- | -- | -- | -- expected_data.csv
 |   |-- | -- | -- | -- Goalies..csv
 |-- Pipfile
 |-- Pipfile.lock
```

## Final Spark Dataframe Output

```bash
+----+----+------------------+------------------+------------------+----------------+--------------+-------------------+----------------------------------------------+---------------------------------------------+
|tmID|Year|Wins_Agg          |Losses_Agg        |GP_Agg            |Mins_over_GA_agg|GA_Over_SA_Agg|Avg_Percentage_Wins|Most_Goals_Stopped                            |Most_Efficient_Player                        |
+----+----+------------------+------------------+------------------+----------------+--------------+-------------------+----------------------------------------------+---------------------------------------------+
|BOS |1948|7.25              |5.75              |15.0              |22.086          |null          |49.537             |{"PlayerId":"sawchte01","Goals_Stopped":103.0}|{"PlayerId":"pronocl01","Efficiency":0.00833}|
|BOS |1999|8.0               |13.0              |30.666666666666668|20.805          |0.106         |25.23              |{"PlayerId":"sawchte01","Goals_Stopped":103.0}|{"PlayerId":"pronocl01","Efficiency":0.00833}|
|OT1 |1910|13.0              |3.0               |16.0              |14.348          |null          |81.25              |{"PlayerId":"benedcl01","Goals_Stopped":61.0} |{"PlayerId":"benedcl01","Efficiency":0.00227}|
|VAN |1974|12.666666666666666|10.666666666666666|32.666666666666664|19.277          |null          |22.815             |{"PlayerId":"luongro01","Goals_Stopped":60.0} |{"PlayerId":"fountmi01","Efficiency":0.00207}|
|VIC |1925|15.0              |11.0              |30.0              |35.075          |null          |50.0               |{"PlayerId":"holmeha01","Goals_Stopped":41.0} |{"PlayerId":"holmeha01","Efficiency":0.00163}|
|CHI |1960|29.0              |24.0              |70.0              |23.333          |null          |41.429             |{"PlayerId":"hallgl01","Goals_Stopped":84.0}  |{"PlayerId":"chabolo01","Efficiency":0.00283}|
|COL |1998|14.666666666666666|9.333333333333334 |28.666666666666668|24.755          |0.088         |43.28              |{"PlayerId":"roypa01","Goals_Stopped":66.0}   |{"PlayerId":"elliobr01","Efficiency":0.00180}|
|NJD |1983|8.5               |28.0              |47.5              |14.084          |0.135         |17.914             |{"PlayerId":"brodema01","Goals_Stopped":119.0}|{"PlayerId":"brodema01","Efficiency":0.00170}|
|NYR |1991|25.0              |12.5              |43.0              |20.184          |0.094         |58.049             |{"PlayerId":"sawchte01","Goals_Stopped":103.0}|{"PlayerId":"chabolo01","Efficiency":0.00283}|
|PHO |2006|7.75              |11.5              |23.75             |17.848          |0.112         |25.682             |{"PlayerId":"josepcu01","Goals_Stopped":51.0} |{"PlayerId":"smithmi02","Efficiency":0.00146}|
|VAN |1989|12.5              |20.5              |42.0              |16.546          |0.122         |26.191             |{"PlayerId":"luongro01","Goals_Stopped":60.0} |{"PlayerId":"fountmi01","Efficiency":0.00207}|
|CHC |1974|7.5               |11.75             |19.75             |15.29           |0.105         |19.375             |{"PlayerId":"drydeda01","Goals_Stopped":17.0} |{"PlayerId":"drydeda01","Efficiency":0.00070}|
|DET |1992|23.5              |14.0              |44.5              |18.515          |0.113         |54.919             |{"PlayerId":"sawchte01","Goals_Stopped":103.0}|{"PlayerId":"gatheda01","Efficiency":0.00556}|
|EDM |1987|11.0              |6.25              |22.5              |17.194          |0.12          |30.0               |{"PlayerId":"josepcu01","Goals_Stopped":51.0} |{"PlayerId":"valiqst01","Efficiency":0.00177}|
|KCS |1974|5.0               |18.0              |27.333333333333332|14.815          |null          |18.098             |{"PlayerId":"herrode01","Goals_Stopped":10.0} |{"PlayerId":"mckenbi01","Efficiency":0.00042}|
|MTL |1996|7.75              |9.0               |23.75             |18.421          |0.098         |22.711             |{"PlayerId":"hainsge01","Goals_Stopped":104.0}|{"PlayerId":"pronocl01","Efficiency":0.00833}|
|OTS |1920|14.0              |10.0              |24.0              |19.493          |null          |58.333             |{"PlayerId":"conneal01","Goals_Stopped":81.0} |{"PlayerId":"conneal01","Efficiency":0.00311}|
|PIP |1927|19.0              |17.0              |44.0              |36.053          |null          |43.182             |{"PlayerId":"wortero01","Goals_Stopped":67.0} |{"PlayerId":"wortero01","Efficiency":0.00222}|
|PIT |1974|9.25              |7.0               |23.0              |16.842          |null          |36.36              |{"PlayerId":"thibajo01","Goals_Stopped":39.0} |{"PlayerId":"conklty01","Efficiency":0.00147}|
|SJS |2011|21.5              |14.5              |43.5              |25.02           |0.085         |48.684             |{"PlayerId":"belfoed01","Goals_Stopped":76.0} |{"PlayerId":"schaeno01","Efficiency":0.00284}|
+----+----+------------------+------------------+------------------+----------------+--------------+-------------------+----------------------------------------------+---------------------------------------------+
```

## Data Assumptions
When compared with data most common player against the same player when they are not common with their most goals scored

- When Most_Efficient_Player or Most Goals Scored player is winklha01, then teams has good Avg_Percentage_Wins
- Only 2 teams this player is associated when they are most goals scored
- For team BOS, player winklha01 has 94 times the highest goals scored

```bash
+-----------------------+----------------------------+----------------------------+----------------------------+----------------------------+
|Most_Efficient_PlayerID|Same_Max_Avg_Percentage_Wins|Same_Min_Avg_Percentage_Wins|Diff_Max_Avg_Percentage_Wins|Diff_Min_Avg_Percentage_Wins|
+-----------------------+----------------------------+----------------------------+----------------------------+----------------------------+
|winklha01              |60.714                      |28.115                      |81.035                      |33.761                      |
|cudewi01               |7.778                       |7.778                       |72.34                       |6.061                       |
|levaslo01              |37.5                        |37.5                        |55.682                      |21.149                      |
+-----------------------+----------------------------+----------------------------+----------------------------+----------------------------+
```
