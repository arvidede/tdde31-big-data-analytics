# Big Data Analytics - BDA2 - 2 api
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

###### TEMPERATURE ######
temperatureFile = sc.textFile("../data/temperature-readings.csv")
temperatureLines = temperatureFile.map(lambda line: line.split(";"))
tempReadings = temperatureLines.map(lambda p: Row(station=p[0], month=int(p[1].split("-")[1]), year=int(p[1].split("-")[0]), value=float(p[3])))
schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.createOrReplaceTempView("tempReadings")

###### Östergötland stations ######
ostergotlandFile = sc.textFile("../data/stations-Ostergotland.csv")
ostergotlandLines= ostergotlandFile.map(lambda line: line.split(";"))
ostergotlandStations = ostergotlandLines.map(lambda p: Row(station=p[0]))
schemaStations = sqlContext.createDataFrame(ostergotlandStations)
schemaStations.createOrReplaceTempView("stations")

###### JOIN ######
readings = schemaTempReadings.join(schemaStations, 'station').orderBy(['year', 'month', 'value'], descending=[1,1,0])

readings = readings.filter(readings.year.between(1950, 2014))

###### Regional average ######
regionalAverages = readings.groupBy('year', 'month') \
                           .agg(F.avg('value').alias('average'))

###### Long term average ######
longTermAverages = readings.filter(readings.year.between(1950, 1980))
longTermAverages = longTermAverages.groupBy('month') \
                                   .agg(F.avg('value')) \
                                   .collect()

###### Temperature difference ######
long_term_averages = dict(longTermAverages)


difference = regionalAverages.rdd.map(lambda x: Row(year=x.year, month=x.month, value=(x.average - long_term_averages[x.month])))


# year, month, difference ORDER BY year DESC, month DESC

difference.toDF().repartition(1).write.mode('append').csv("./results/lab2_6")
