# Big Data Analytics - BDA2 - 2 api
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

###### TEMPERATURE ######
temperatureFile = sc.textFile("../data/temperature-readings.csv")
temperatureLines = temperatureFile.map(lambda line: line.split(";"))
tempReadings = temperatureLines.map(lambda p: Row(station=p[0], value=float(p[3])))
schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.createOrReplaceTempView("tempReadings")

# Highest daily temperature per station
schemaTempReadings = schemaTempReadings.groupBy('station') \
                                        .agg(F.max('value').alias('maxTemp'))

schemaTempReadings = schemaTempReadings.filter(schemaTempReadings.maxTemp.between(25, 30))




###### PRECIPITATION ######
precipitationFile = sc.textFile("../data/precipitation-readings.csv")
precipitationLines = precipitationFile.map(lambda line: line.split(";"))
precipitationReadings = precipitationLines.map(lambda p: Row(station=p[0], date=p[1], value=float(p[3])))
schemaPrecipitationReadings = sqlContext.createDataFrame(precipitationReadings)
schemaPrecipitationReadings.createOrReplaceTempView("precipitationReadings")

schemaPrecipitationReadings = schemaPrecipitationReadings.groupBy(['date', 'station']) \
                                        .agg(F.sum('value').alias('stationDaySum')) \

schemaPrecipitationReadings = schemaPrecipitationReadings.groupBy(['station']) \
                                                        .agg(F.max('stationDaySum').alias('maxDailyPrecipitation'))

schemaPrecipitationReadings = schemaPrecipitationReadings.filter(schemaPrecipitationReadings.maxDailyPrecipitation.between(100, 200))


# ###### JOIN ######
readings = schemaTempReadings.join(schemaPrecipitationReadings, 'station').orderBy(['station', 'maxTemp', 'maxDailyPrecipitation'], descending=[1,0,0,1])


# # Huanyu: The key for two rdds to join is supposed to be just station. Date is not needed here.
# # year, month, station number, average value ORDER BY year, month ASC

readings.repartition(1).write.mode('append').csv("./results/lab2_4")
