from pyspark import SparkContext

# Misc input and setup
sc = SparkContext(appName = "lab1_6")

# Local
station_file = sc.textFile("./data/stations-Ostergotland.csv")
temperature_file = sc.textFile("./data/temperature-readings.csv")

# Heffa
# station_file = sc.textFile("/user/x_arved/data/stations-Ostergotland.csv")
# precipitation_file = sc.textFile("/user/common/732A54/precipitation-readings.csv")


station_lines = station_file.map(lambda line: line.split(";"))
temperature_lines = temperature_file.map(lambda line: line.split(";"))


# Station number as list
stations = station_lines.map(lambda x: (x[0])).collect()


# Date = YYYY-MM [0:7]
# Date, station number, temperature, iterator
temperature_readings = temperature_lines.map(lambda x: ((x[1][0:7], x[0]), (float(x[3]), 1)))

# Filter all readings from stations in Östergötland before 1950 and after 2014
station_readings = temperature_readings.filter(lambda x: \
                            x[0][1] in stations
                            and int(x[0][0][0:4])>=1950
                            and int(x[0][0][0:4])<=2014)

# This is the long term averages for each month 1950-1980.
# Remove all values after 1980 and map to remove station number and year from key.
# Reduce by month
# Collect to array of key-values (month, temp) (should be size 12)
long_term_averages = station_readings.filter(lambda x: int(x[0][0][0:4])<=1980) \
                                    .map(lambda x: (x[0][0][5:7], (x[1][0], x[1][1]))) \
                                    .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) \
                                    .mapValues(lambda v: v[0]/v[1]) \
                                    .collect()

# Convert to dict with month as key
long_term_averages = dict(long_term_averages)
print(long_term_averages)

# Map to remove station number from key. Reduce by date sum all temperatures and divide by n.
regional_averages = station_readings \
                    .map(lambda x: (x[0][0], (x[1][0], x[1][1]))) \
                    .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) \
                    .mapValues(lambda v: v[0]/v[1]) \
                    .sortByKey(numPartitions = 1)

# comparison = regional_averages.map(lambda x: (x[0], (x[1] - long_term_averages[x[0][5:7]]))) \
#                     .sortByKey(numPartitions = 1)

# Save to file
regional_averages.saveAsTextFile("lab1_6_readings")
