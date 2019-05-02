from pyspark import SparkContext

# Misc input and setup
sc = SparkContext(appName = "lab_1_2_a")

# Heffa
temperature_file = sc.textFile("/user/x_arved/data/temperature-readings.csv")

# Local
# temperature_file = sc.textFile("./data/temperature-readings.csv")

lines = temperature_file.map(lambda line: line.split(";"))
year_temperature = lines.map(lambda x: ((x[1][0:4], x[1][5:7]), float(x[3])))

# Filter all readings before 1950 and after 2014 which has a temperature above 10 degrees
year_temperature = year_temperature.filter(lambda x: int(x[0][0])>=1950 and int(x[0][0])<=2014 and x[1] > 10)

# occurences = year_temperature.countByKey()

# f = open('/user/x_arved/results/results_1_2_a.txt', 'w+')
# for keys, count in occurences.items():
#     f.write(str(keys[0]) + ',' + str(keys[1]) + ',' + str(count) + '\n')
# f.close()

occurences = year_temperature \
            .map(lambda x: ((x[0][0], x[0][1]), 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .sortByKey(numPartitions = 1)

occurences.saveAsTextFile('lab1_2_a')
