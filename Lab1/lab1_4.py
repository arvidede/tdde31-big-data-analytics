from pyspark import SparkContext

# Misc input and setup
sc = SparkContext(appName = "lab1_4")

# Local
temperature_file = sc.textFile("./data/temperature-readings.csv")
precipitation_file = sc.textFile("./data/precipitation-readings.csv")

# Heffa
# temperature_file = sc.textFile("/user/x_arved/data/temperature-readings.csv")
# precipitation_file = sc.textFile("/user/common/732A54/precipitation-readings.csv")

temperature_lines = temperature_file.map(lambda line: line.split(";"))
precipitation_lines = precipitation_file.map(lambda line: line.split(";"))

# Station number, date, temperature
temperature_readings = temperature_lines.map(lambda x: ((x[0], x[1]), float(x[3])))

# Station number, date, precipitation
precipitation_readings = precipitation_lines.map(lambda x: ((x[0], x[1]), float(x[3])))

# Sum precipitations for each day
precipitation_readings = precipitation_readings.reduceByKey(lambda a,b: (a + b))

# Max temperature & precipitation
temperature_max = temperature_readings.reduceByKey(lambda a,b: a if a>=b else b)
precipitation_max = precipitation_readings.reduceByKey(lambda a,b: a if a>=b else b)

# Filter all readings with temp between 25 and 30
temperature_readings = temperature_readings.filter(lambda x: x[1]>=25 and x[1]<=30)

# Filter all readings with precipitation between 100 and 200
precipitation_readings = precipitation_readings.filter(lambda x: x[1]>=100 and x[1]<=200)

# Join by key (station, date)
filtered_data = temperature_max.join(precipitation_max).repartition(1)

# Save to file
filtered_data.saveAsTextFile("lab1_4")
