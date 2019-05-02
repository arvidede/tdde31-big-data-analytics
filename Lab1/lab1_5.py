from pyspark import SparkContext

# Misc input and setup
sc = SparkContext(appName = "lab1_5")

# Local
# station_file = sc.textFile("./data/stations-Ostergotland.csv")
# precipitation_file = sc.textFile("./data/precipitation-readings.csv")

# Heffa
station_file = sc.textFile("/user/x_arved/data/stations-Ostergotland.csv")
precipitation_file = sc.textFile("/user/common/732A54/precipitation-readings.csv")


station_lines = station_file.map(lambda line: line.split(";"))
precipitation_lines = precipitation_file.map(lambda line: line.split(";"))


# Station number as list
stations = station_lines.map(lambda x: (x[0])).collect()


# Date = YYYY-MM [0:7]
# Station number, date, precipitation
precipitation_readings = precipitation_lines.map(lambda x: ((x[0], x[1][0:7]), (float(x[3]), 1)))


# Filter all readings from stations in Östergötland before 1950 and after 2014
precipitation_readings = precipitation_readings.filter(lambda x: \
                            x[0][0] in stations
                            and int(x[0][1][0:4])>=1993
                            and int(x[0][1][0:4])<=2016)


# Reduce by (station, date), calculate average per station, reduce by date, calculate average per month
average_precipitations = precipitation_readings \
                    .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) \
                    .map(lambda x: (x[0][1] ,(x[1][0]/x[1][1], 1))) \
                    .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) \
                    .mapValues(lambda v: v[0]/v[1]) \
                    .sortByKey(numPartitions=0)


# Save to file
average_precipitations.saveAsTextFile("lab1_5")
