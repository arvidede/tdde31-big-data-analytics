# Big Data Analytics - BDA2 - 2 api
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

# Load a text file and convert each line to a Row.
rdd = sc.textFile("../data/temperature-readings.csv")
parts = rdd.map(lambda l: l.split(";"))
tempReadings = parts.map(lambda p: Row(month=p[1].split("-")[1], year=p[1].split("-")[0], value=float(p[3])))

schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")

schemaTempReadings = schemaTempReadings.filter(schemaTempReadings["year"] >= 1950 & schemaTempReadings["year"] <= 2014 & schemaTempReadings["value"] > 10)

# Distinct
schemaTempReadingsDistinct = schemaTempReadings.groupBy('year', 'month') \
                                          .agg(F.count('value').alias('distinct')) \
                                          .select(['year', 'month', 'distinct']) \
                                          .orderBy(['year', 'month', 'distinct'], descending=[0,0,1])

# Not distinct
schemaTempReadingsNotDistinct = schemaTempReadings.groupBy('year', 'month') \
                                          .agg(F.countDistinct('value').alias('notDistinct')) \
                                          .select(['year', 'month', 'notDistinct']) \
                                          .orderBy(['year', 'month', 'notDistinct'], descending=[0,0,1])

# year, month, value ORDER BY value DESC
# year, month, value ORDER BY value DESC

schemaTempReadingsDistinct.write.mode('append').csv("./results/lab2_2_api_distinct")
schemaTempReadingsNotDistinct.write.mode('append').csv("./results/lab2_2_api_not_distinct")
