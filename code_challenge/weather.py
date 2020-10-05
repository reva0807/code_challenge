from code_challenge.utils.spark_singleton import SparkSingleton

spark=SparkSingleton.get_instance()

from pyspark.sql.functions import col,regexp_replace,when,avg,row_number, max as _max
from pyspark.sql.window import Window

from pyspark.sql.functions import udf
# import re
# Step 1 - Setting Up the Data

#1. Load the global weather data into your big data technology of choice.
# can not convert to schema listed in readme due to MAX and MIN cols can't be casted to float
weather_df=spark.read.option("header","true").csv("data/2019/")

# clean '*' in MAX and MIN cols
weather_df= weather_df.withColumn("MAX",regexp_replace(col("MAX"),"\*",""))\
                      .withColumn("MIN",regexp_replace(col("MIN"),"\*",""))

# todo udf to convert missing value 9's to null
# python version is not compatible with pyspark setting. give up this approach for the challenge.
'''

@udf("float")
def missing_to_null(str_value):
    if re.match(r"^9+.{1}9+$",str_value)!=None:
        return 0

weather_df.withColumn("test",missing_to_null("TEMP")).show()
'''

# 2. Join the stationlist.csv with the countrylist.csv to get the full country name
#     for each station number.
station_df=spark.read.option("header","true").csv("stationlist.csv")

country_df=spark.read.option("header","true").csv("countrylist.csv")

station_country_df=station_df.join(country_df,"COUNTRY_ABBR","left")

# 3. Join the global weather data with the full country names by station number.

df=weather_df.join(station_country_df,col("`STN---`")==col("STN_NO"),"left")

# Step 2 - Analyzing data

# Which country had the hottest average mean temperature over the year?
max_temp=df.withColumn("TEMP",when(col("TEMP")=="9999.9",None).otherwise(col("TEMP")))\
    .orderBy('TEMP',ascending=False)\
    .select("TEMP")\
    .first()[0]

country_with_max_temp=df.filter(col("TEMP")==max_temp).select("country_full","TEMP").distinct()
country_with_max_temp.show()

#Which country had the most consecutive days of tornadoes/funnel cloud formations?

funnel_df=df.withColumn("has_funnel",col("FRSHTT").substr(-1,1).cast("boolean"))
w1=Window.partitionBy(col("COUNTRY_ABBR")).orderBy(col("YEARMODA"))
w2=Window.partitionBy(col("COUNTRY_ABBR"),col("has_funnel")).orderBy(col("YEARMODA"))

res=funnel_df.withColumn('grp',row_number().over(w1)-row_number().over(w2))
w3 = Window.partitionBy("COUNTRY_ABBR","has_funnel","grp").orderBy("YEARMODA")

streak_res = res.withColumn('streak_1',when(col("has_funnel") == 0,0).otherwise(row_number().over(w3)))

max_consecutive_days=streak_res.orderBy('streak_1', ascending=False).select(_max(col("streak_1"))).first()[0]

max_consecutive_df=streak_res.filter(col("streak_1")==max_consecutive_days)\
    .selectExpr("country_full","streak_1 AS consecutive_funnel_days")\
    .distinct()

max_consecutive_df.show()

#Which country had the second highest average mean wind speed over the year?
avg_wdsp_df=df.withColumn("WDSP",(when(col("WDSP")=="999.9",None).otherwise(col("WDSP"))).cast("float"))\
.groupBy("country_full")\
.agg(avg("WDSP").alias("AVG_WDSP"))

second_avg_wdsp=avg_wdsp_df.orderBy('AVG_WDSP', ascending=False)\
.select('AVG_WDSP')\
.distinct()\
.collect()[2][0]

country_with_second_avg_wdsp=avg_wdsp_df.filter(col("AVG_WDSP")==second_avg_wdsp)\
    .select("country_full","AVG_WDSP")\
    .distinct()
country_with_second_avg_wdsp.show()
