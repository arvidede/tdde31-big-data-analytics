from pyspark import SparkContext

# Misc input and setup
sc = SparkContext(appName = "assignment 1")
temperature_file = sc.textFile("/user/x_arved/data/temperature_readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))
year_temperature = lines.map(lambda x: (x[0], x[1][0:4], float(x[3])))

# Filter all readings before 1950 and after 2014
year_temperature = year_temperature.filter(lamdba x: int(x[0])>=1950 and int(x[0])<=2014)

# MapReduce, reduce by temperature
max_temperatures = year_temperature.reduceByKey(lambda a,b: a if a>=b else b)
min_temperatures = year_temperature.reduceByKey(lambda a,b: a if a<b else b)

# Sort by temperature, index = 2
max_temperatures_sorted = max_temperatures.sortBy(ascending False, keyfunc = lambd k: k[2])
min_temperatures_sorted = min_temperatures.sortBy(ascending False, keyfunc = lambd k: k[2])

# Save to file
max_temperatures_sorted.savAasTextFile("max_temperature")
min_temperatures_sorted.savAasTextFile("min_temperature")
