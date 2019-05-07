# Big Data Analytics - BDA2
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

# Load a text file and convert each line to a Row.
rdd = sc.textFile("../data/temperature-readings.csv")
parts = rdd.map(lambda l: l.split(";"))
tempReadings = parts.map(lambda p: Row(station=p[0], date=p[1], year=p[1].split("-")[0], time=p[2], value=float(p[3]), quality=p[4]))

schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")

# Lowest and highest temperature measured each year.
max = sqlContext.sql("SELECT year, station, max(value) as value FROM tempReadings WHERE year>=1950 AND year<=2014 ORDER BY value DESC")
min = sqlContext.sql("SELECT year, station, min(value) as value FROM tempReadings WHERE year>=1950 AND year<=2014 ORDER BY value DESC")

# largerThan10Degrees = sqlContext.sql("SELECT year, month, count(value) as value FROM tempReadingsTable WHERE year=1950 and value>=10.0 group by year, month")

# year, station with the max, maxValue ORDER BY maxValue DESC
# year, station with the min, minValue ORDER BY minValue DESC
max.write.mode('append').csv("./results/lab2_1_max")
min.write.mode('append').csv("./results/lab2_1_min")
