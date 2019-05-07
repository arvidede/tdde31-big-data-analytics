# Big Data Analytics - BDA2 - 1
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
schemaTempReadingsMax = schemaTempReadings.groupBy('year', 'station') \
                                          .agg(F.min('value') \
                                          .alias('dailymax')) \
                                          .orderBy(['year', 'station', 'dailymax'], ascending=[0,0,1])

schemaTempReadingsMin = schemaTempReadings.groupBy('year', 'station') \
                                          .agg(F.max('value') \
                                          .alias('dailymin')) \
                                          .orderBy(['year', 'station', 'dailymin'], ascending=[0,0,1])


#max = sqlContext.sql("SELECT year, station, max(value) as value FROM tempReadings WHERE year>=1950 AND year<=2014 ORDER BY value DESC")
#min = sqlContext.sql("SELECT year, station, min(value) as value FROM tempReadings WHERE year>=1950 AND year<=2014 ORDER BY value DESC")

# largerThan10Degrees = sqlContext.sql("SELECT year, month, count(value) as value FROM tempReadingsTable WHERE year=1950 and value>=10.0 group by year, month")

# year, station with the max, maxValue ORDER BY maxValue DESC
# year, station with the min, minValue ORDER BY minValue DESC

schemaTempReadingsMax.write.mode('append').csv("./results/lab2_1_max")
schemaTempReadingsMin.write.mode('append').csv("./results/lab2_1_min")
