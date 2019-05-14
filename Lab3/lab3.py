# BDA3 - Machine learning with spark
from future import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext

#sc = SparkContext(appName="lab_kernel")
sc = SparkContext()
sqlContext = SQLContext(sc)
dateFormat = '%y-%m-%d'

# Stations - station, lat, long
stationsFile = sc.textFile("../data/stations.csv")
stationsLines= stationsFile.map(lambda line: line.split(";"))
stations = stationsLines.map(lambda x: ((x[0]), (float(x[3]), float(x[4]))))
stations = dict(stations.collect())

# Temperatures - station, date, time, temp, lat, long
temperatureFile = sc.textFile("../data/temperature-readings.csv")
temperatureLines = temperatureFile.map(lambda line: line.split(";"))
tempReadings = temperatureLines.map(lambda x: ((x[0], datetime.strptime(x[1], dateFormat), int(x[2][0:2]), float(x[3]), stations[x[0]][0], stations[x[0]][1]))

# Exclude all dates after 2013-07-04
readings = readings.filter(lambda x: (int(x[1].year)<=2013 and int(x[1].month)<=7 and int(x[1].day)<=4))

def haversine(lon1, lat1, lon2, lat2):
    # Calculate the great circle distance between two points on the earth (specified in decimal degrees)
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km

# Up to you
h_distance = 100000
h_date = 10
h_time = 2
a = 58.4274
b = 14.826
date = datetime.strptime("2013-07-04", dateFormat)
h = (4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 0)

# Your code here
# Gaussian kernel
def kernel(u):
    return(exp(-(abs(u)**2)))

# Date
def dateDiff(x):
    return(abs(x.days % 365))


gaussianKernel = []
for hour in h:
    # station, date, time, temp, lat, long
    kernels = readings.map(lambda x: (kernel(haversine(x[5], x[4], b, a) / h_distance), \
                                      kernel(dateDiff(date - x[1]) / h_date), \
                                      kernel((abs(hour - x[2]) % 12) / h_time), \
                                      x[3]))

    kernelSum = kernels.map(lambda x: (((x[0] + x[1] + x[2])*x[3]) / (x[0] + x[1] + x[2])))
    gaussianKernel.append(kernelSum)

print gaussianKernel
