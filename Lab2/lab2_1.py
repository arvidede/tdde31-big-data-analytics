# Big Data Analytics - BDA2 - 1
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

# Load a text file and convert each line to a Row.
rdd = sc.textFile("../data/temperature-readings.csv")
parts = rdd.map(lambda l: l.split(";"))
tempReadings = parts.map(lambda p: Row(station=p[0], year=int(p[1].split("-")[0]), value=float(p[3])))

schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")

schemaTempReadings = schemaTempReadings.filter("year >= 1950 AND year <= 2014")

# Lowest and highest temperature measured each year.
schemaTempReadingsMax = schemaTempReadings.groupBy('year') \
                                          .agg(F.max('value').alias('value'))

schemaTempReadingsMax = schemaTempReadings.join(schemaTempReadingsMax, ['year', 'value']).orderBy(['year', 'station', 'value'], descending=[0,0,1])



schemaTempReadingsMin = schemaTempReadings.groupBy('year') \
                                          .agg(F.min('value').alias('value'))

schemaTempReadingsMin = schemaTempReadings.join(schemaTempReadingsMin, ['year', 'value']).orderBy(['year', 'station', 'value'], descending=[0,0,1])


# year, station with the max, maxValue ORDER BY maxValue DESC
# year, station with the min, minValue ORDER BY minValue DESC

schemaTempReadingsMax.repartition(1).write.mode('append').csv("./results/lab2_1_max")
schemaTempReadingsMin.repartition(1).write.mode('append').csv("./results/lab2_1_min")
