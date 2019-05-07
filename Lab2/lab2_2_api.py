# Big Data Analytics - BDA2 - 2 api
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

# Load a text file and convert each line to a Row.
rdd = sc.textFile("../data/temperature-readings.csv")
parts = rdd.map(lambda l: l.split(";"))
tempReadings = parts.map(lambda p: Row(station=p[0], year=p[1].split("-")[0], value=float(p[3])))

schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")

# Lowest and highest temperature measured each year.
schemaTempReadingsDistinct = schemaTempReadings.groupBy('year', 'station') \
                                          .agg(F.min('value') \
                                          .alias('dailymax')) \
                                          .orderBy(['year', 'station', 'dailymax'], ascending=[0,0,1])

schemaTempReadingsNotDistinct = schemaTempReadings.groupBy('year', 'station') \
                                          .agg(F.max('value') \
                                          .alias('dailymin')) \
                                          .orderBy(['year', 'station', 'dailymin'], ascending=[0,0,1])

# Not distinct
largerThan10Degrees = sqlContext.sql("SELECT year, month, count(value) as value FROM tempReadingsTable WHERE year=1950 and value>=10.0 group by year")

# Distinct
largerThan10Degrees = sqlContext.sql("SELECT year, month, count(value) as value FROM tempReadingsTable WHERE year=1950 and value>=10.0 group by year")

# year, station with the max, maxValue ORDER BY maxValue DESC
# year, station with the min, minValue ORDER BY minValue DESC

schemaTempReadingsMax.write.mode('append').csv("./results/lab2_1_max")
schemaTempReadingsMin.write.mode('append').csv("./results/lab2_1_min")
