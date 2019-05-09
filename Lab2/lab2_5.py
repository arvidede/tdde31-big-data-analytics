# Big Data Analytics - BDA2 - 2 api
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

stationFile = sc.textFile("../data/stations-Ostergotland.csv")
precipitationFile = sc.textFile("../data/precipitation-readings.csv")

precipitationLines = precipitationFile.map(lambda line: line.split(";"))
precipitationReadings = precipitationLines.map(lambda p: Row(station=str(p[0]), year=int(p[1].split('-')[0]), month=int(p[1].split("-")[1]), value=float(p[3])))
schemaPrecipitationReadings = sqlContext.createDataFrame(precipitationReadings)
schemaPrecipitationReadings.createOrReplaceTempView("precipitationReadings")

stationLines = stationFile.map(lambda line: line.split(";"))

# Station numbers as list
stations = stationLines.map(lambda x: (x[0])).collect()

# # Filter all readings from stations in Östergötland before 1993 and after 2014
schemaPrecipitationReadings = schemaPrecipitationReadings.filter( \
                            schemaPrecipitationReadings['station'].isin(stations) & \
                            schemaPrecipitationReadings['year'].between(1993,2016))


schemaPrecipitationReadings = schemaPrecipitationReadings.groupBy(['year', 'month', 'station']) \
                                        .agg(F.sum('value').alias('stationMonthSum'))

schemaPrecipitationReadings = schemaPrecipitationReadings.groupBy(['year', 'month']) \
                                        .agg(F.avg('stationMonthSum').alias('avgMonthlyPrecipitation')) \
                                        .orderBy(['year', 'month','avgMonthlyPrecipitation'], descending=[1,1,0]) \


# year, month, average value ORDER BY year, month DESC

schemaPrecipitationReadings.repartition(1).write.mode('append').csv("./results/lab2_5")
