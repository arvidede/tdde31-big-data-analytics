# Big Data Analytics - BDA2 - 2 sql
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

# Load a text file and convert each line to a Row.
rdd = sc.textFile("../data/temperature-readings.csv")
parts = rdd.map(lambda l: l.split(";"))
tempReadings = parts.map(lambda p: Row(station=p[0], year=int(p[1].split("-")[0]), month=int(p[1].split("-")[1]), value=float(p[3])))

schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.createOrReplaceTempView("tempReadings")

# Not distinct
allFilteredReadings = (sqlContext.sql("SELECT year, month, COUNT(value) FROM tempReadings WHERE year>=1950 AND year<=2014 AND value>=10.0 GROUP BY year, month ORDER BY year, month"))

# Distinct
distinctFilteredReadings = sqlContext.sql("SELECT year, month, COUNT(value) FROM (SELECT DISTINCT station, year, month, value FROM tempReadings WHERE year>=1950 AND year<=2014 AND value>=10.0 GROUP BY year, month, station, value) GROUP BY year, month ORDER BY year, month")

# year, station with the max, maxValue ORDER BY maxValue DESC
# year, station with the min, minValue ORDER BY minValue DESC

allFilteredReadings.repartition(1).write.mode('append').csv("./results/lab2_2_sql")
distinctFilteredReadings.repartition(1).write.mode('append').csv("./results/lab2_2_sql_distinct")
