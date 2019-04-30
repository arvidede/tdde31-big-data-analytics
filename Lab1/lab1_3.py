from pyspark import SparkContext

# Misc input and setup
sc = SparkContext(appName = "lab1_3")

# Local
# temperature_file = sc.textFile("./data/temperature-readings.csv")

# Heffa
temperature_file = sc.textFile("/user/x_arved/data/temperature-readings.csv")
# temperature_file = sc.textFile("/user/x_arved/data/temperatures-big.csv")
# temperature_file = sc.textFile("/user/common/732A54/temperatures-big.csv")

lines = temperature_file.map(lambda line: line.split(";"))

# Year, month, station number, average monthly temperature
# year_temperature = lines.map(lambda x: ((x[1][0:4], x[1][5:7], x[0],), float(x[3])))
year_temperature = lines.map(lambda x: ((x[1][0:7], x[0]), (float(x[3]), 1)))

# Filter all readings before 1950 and after 2014
year_temperature = year_temperature.filter(lambda x: int(x[0][0][0:4])>=1960 and int(x[0][0][0:4]) <=2014)

# MapReduce, reduce by temperature
average_temperatures = year_temperature \
                    .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) \
                    .mapValues(lambda v: v[0]/v[1]) \
                    .repartition(1)
# Save to file
average_temperatures.saveAsTextFile("average_temperature")
