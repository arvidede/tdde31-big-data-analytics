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
# Station number, date, precipitation, iterator
precipitation_readings = precipitation_lines.map(lambda x: ((x[0], x[1][0:7]), float(x[3])))

# FOR ALTERNATIVE SOLUTION, SEE BELOW
# Station number, date, precipitation, iterator
# precipitation_readings = precipitation_lines.map(lambda x: ((x[0], x[1][0:7]), (float(x[3]), 1)))


# Filter all readings from stations in Östergötland before 1950 and after 2014
precipitation_readings = precipitation_readings.filter(lambda x: \
                            x[0][0] in stations
                            and int(x[0][1][0:4])>=1993
                            and int(x[0][1][0:4])<=2016)


# Reduce by (station, date), calculate average per station, reduce by date, calculate average per month
# 1. Sum of precipitations per month and station
# 2. Remove station from key, and add iterator (used to sum for all stations in month)
# 3. Sum all station precipitations
# 4. Calculate average precipitations
average_precipitations = precipitation_readings \
                    .reduceByKey(lambda a,b: (a+b)) \
                    .map(lambda x: (x[0][1] ,(x[1], 1))) \
                    .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) \
                    .mapValues(lambda v: v[0]/v[1]) \
                    .repartition(1)



# ALTERNATIVE SOULTION, USED IN FIRST HAND-IN
# Reduce by (station, date), calculate average per station, reduce by date, calculate average per month
# 1. Sum of precipitations per month and station
# 2. Remove station from key, calculate average per station and add iterator (used to sum for all stations in month)
# 3. Sum all station averages
# 4. Calculate average of station averages
# average_precipitations = precipitation_readings \
#                     .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) \
#                     .map(lambda x: (x[0][1] ,(x[1][0]/x[1][1], 1))) \
#                     .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) \
#                     .mapValues(lambda v: v[0]/v[1]) \
#                     .repartition(1)


# Save to file
average_precipitations.saveAsTextFile("lab1_5_pt2")
