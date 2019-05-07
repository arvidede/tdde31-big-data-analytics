from pyspark import SparkContext

# Misc input and setup
sc = SparkContext(appName = "1_1_test")

# Local
temperature_file = sc.textFile("./data/temperature-readings.csv")

# Heffa
# temperature_file = sc.textFile("/user/x_arved/data/temperature-readings.csv")

lines = temperature_file.map(lambda line: line.split(";"))
year_temperature = lines.map(lambda x: ((x[0], x[1][0:7]), float(x[3])))

year_temperature = year_temperature.filter(lambda x: int(x[0][1][0:4])==2010 and x[0][0] == '95530')

# Sort by temperature, index = 2
year_temperature = year_temperature.sortBy(ascending=False, keyfunc = lambda k: k[1], numPartitions = 1)

# Save to file
year_temperature.saveAsTextFile("1_1_test")
