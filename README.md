#Data Engineer Hockey Aggregates

##Introduction
____
This document is designed to elucidate the process of `data-engineer-hockey-aggregates` component. Technical details are highlighted to seamlessly run the project.

Following are addressed by this component 

- Read [Hockey Data Set](https://www.kaggle.com/open-source-sports/professional-hockey-database?select=Goalies.csv) dataset with Apache Spark 2.4 using pySpark API
- Perform Aggregates on dataframe
- Calculate overall most efficient placer for each hockey team

##Python Packages
____
- Python Version 3.7
- pySpark Version 2.4.0
- pySpark Stubs version 2.4.0

##Project Structure
____
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
 |   |-- | -- | -- main.py
 |   |-- test 
 |   Pipfile
 |   Pipfile.lock
```

##Final Spark Dataframe Output
____
```bash
+----+----+------------------+------------------+------------------+------------------+-------------------+-------------------+---------------------------------------------+---------------------------------------------+
|tmID|Year|Wins_Agg          |Losses_Agg        |GP_Agg            |Mins_over_GA_agg  |GA_Over_SA_Agg     |Avg_Percentage_Wins|Most_Goals_Stopped                           |Most_Efficient_Player                        |
+----+----+------------------+------------------+------------------+------------------+-------------------+-------------------+---------------------------------------------+---------------------------------------------+
|ALB |1972|19.0              |18.5              |42.0              |18.805555555555557|0.10132689987937274|46.875             |{"PlayerId":"norrija01","Goals_Stopped":7.0} |{"PlayerId":"brownke01","Efficiency":0.00119}|
|ANA |1993|11.0              |15.333333333333334|30.0              |20.970954356846473|0.09247889485801995|34.72527472527472  |{"PlayerId":"gigueje01","Goals_Stopped":36.0}|{"PlayerId":"bryzgil01","Efficiency":0.00131}|
|ANA |1994|8.0               |13.5              |28.5              |18.367088607594937|0.1                |26.495726495726494 |{"PlayerId":"gigueje01","Goals_Stopped":36.0}|{"PlayerId":"bryzgil01","Efficiency":0.00131}|
|ANA |1995|17.5              |19.5              |44.5              |20.50826446280992 |0.09187547456340167|35.3954802259887   |{"PlayerId":"gigueje01","Goals_Stopped":36.0}|{"PlayerId":"bryzgil01","Efficiency":0.00131}|
|ANA |1996|12.0              |11.0              |30.666666666666668|21.90748898678414 |0.08463832960477256|24.15008291873964  |{"PlayerId":"gigueje01","Goals_Stopped":36.0}|{"PlayerId":"bryzgil01","Efficiency":0.00131}|
|ANA |1997|8.666666666666666 |14.333333333333334|31.0              |19.76984126984127 |0.10149013290374546|20.253623188405797 |{"PlayerId":"gigueje01","Goals_Stopped":36.0}|{"PlayerId":"bryzgil01","Efficiency":0.00131}|
|ANA |1998|11.666666666666666|11.333333333333334|29.0              |24.58910891089109 |0.0779320987654321 |22.383252818035427 |{"PlayerId":"gigueje01","Goals_Stopped":36.0}|{"PlayerId":"bryzgil01","Efficiency":0.00131}|
|ANA |1999|17.0              |18.0              |44.0              |22.770642201834864|0.09688888888888889|35.58823529411765  |{"PlayerId":"gigueje01","Goals_Stopped":36.0}|{"PlayerId":"bryzgil01","Efficiency":0.00131}|
|ANA |2000|6.25              |11.5              |22.5              |20.704166666666666|0.09950248756218906|19.251462311003202 |{"PlayerId":"gigueje01","Goals_Stopped":36.0}|{"PlayerId":"bryzgil01","Efficiency":0.00131}|
|ANA |2001|9.666666666666666 |15.0              |29.0              |25.842931937172775|0.085040071237756  |21.66952544311035  |{"PlayerId":"gigueje01","Goals_Stopped":36.0}|{"PlayerId":"bryzgil01","Efficiency":0.00131}|
|ANA |2002|20.0              |16.5              |43.5              |27.054347826086957|0.0777027027027027 |39.790209790209786 |{"PlayerId":"gigueje01","Goals_Stopped":36.0}|{"PlayerId":"bryzgil01","Efficiency":0.00131}|
|ANA |2003|9.666666666666666 |14.333333333333334|29.333333333333332|24.116504854368934|0.08456486042692939|55.09469696969697  |{"PlayerId":"gigueje01","Goals_Stopped":36.0}|{"PlayerId":"bryzgil01","Efficiency":0.00131}|
|ANA |2005|21.5              |13.5              |45.5              |22.944444444444443|0.0890721649484536 |45.96774193548387  |{"PlayerId":"gigueje01","Goals_Stopped":36.0}|{"PlayerId":"bryzgil01","Efficiency":0.00131}|
|AND |2006|12.0              |5.0               |22.0              |25.55897435897436 |0.08685968819599109|37.830687830687836 |{"PlayerId":"gigueje01","Goals_Stopped":36.0}|{"PlayerId":"ellisda01","Efficiency":0.00136}|
|AND |2007|15.666666666666666|9.0               |30.0              |27.97752808988764 |0.07759372275501308|42.01510355933144  |{"PlayerId":"gigueje01","Goals_Stopped":36.0}|{"PlayerId":"ellisda01","Efficiency":0.00136}|
|AND |2008|21.0              |16.5              |46.0              |21.876106194690266|0.09072661581694098|45.65217391304348  |{"PlayerId":"gigueje01","Goals_Stopped":36.0}|{"PlayerId":"ellisda01","Efficiency":0.00136}|
|AND |2009|13.0              |10.666666666666666|29.666666666666668|21.226495726495727|0.08577712609970674|40.282485875706215 |{"PlayerId":"gigueje01","Goals_Stopped":36.0}|{"PlayerId":"ellisda01","Efficiency":0.00136}|
|AND |2010|9.4               |6.0               |18.8              |22.06222222222222 |0.08519500189322227|42.63422291993721  |{"PlayerId":"gigueje01","Goals_Stopped":36.0}|{"PlayerId":"ellisda01","Efficiency":0.00136}|
|AND |2011|8.5               |9.0               |22.0              |23.04186046511628 |0.09117896522476675|56.18150684931507  |{"PlayerId":"gigueje01","Goals_Stopped":36.0}|{"PlayerId":"ellisda01","Efficiency":0.00136}|
|ATF |1972|12.5              |19.0              |40.0              |19.66386554621849 |null               |30.626598465473144 |{"PlayerId":"bouchda01","Goals_Stopped":27.0}|{"PlayerId":"bouchda01","Efficiency":0.00071}|
+----+----+------------------+------------------+------------------+------------------+-------------------+-------------------+---------------------------------------------+---------------------------------------------+
```
