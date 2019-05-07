# Big Data Analytics - BDA2 - 2 api
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

# Load a text file and convert each line to a Row.
rdd = sc.textFile("../data/temperature-readings.csv")
parts = rdd.map(lambda l: l.split(";"))
tempReadings = parts.map(lambda p: Row(station=p[0], month=int(p[1].split("-")[1]), year=int(p[1].split("-")[0]), value=float(p[3])))

schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.createOrReplaceTempView("tempReadings")

schemaTempReadings = schemaTempReadings.filter(schemaTempReadings.year.between(1960, 2014))

# Distinct
schemaTempReadings = schemaTempReadings.groupBy('year', 'month', 'station') \
                                        .avg('value') \
                                        .orderBy(['station', 'year', 'month','avg(value)'], ascending=[1,1,1,0]) \
                                        .repartition(1)

# year, month, station number, average value ORDER BY year, month ASC

schemaTempReadings.write.mode('append').csv("./results/lab2_3")
