# BDA3 - Machine learning with spark
from future import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext

#sc = SparkContext(appName="lab_kernel")
sc = SparkContext()
sqlContext = SQLContext(sc)

# Stations
ostergotlandFile = sc.textFile("../data/stations.csv")
ostergotlandLines= ostergotlandFile.map(lambda line: line.split(";"))
ostergotlandStations = ostergotlandLines.map(lambda p: Row(station=p[0], lat=float(p[3]), long=float(p[4]))
schemaStations = sqlContext.createDataFrame(ostergotlandStations)
schemaStations.createOrReplaceTempView("stations")

# Temperatures
temperatureFile = sc.textFile("../data/temperature-readings.csv")
temperatureLines = temperatureFile.map(lambda line: line.split(";"))
tempReadings = temperatureLines.map(lambda p: Row(station=p[0], day=int(p[1].split("-")[2]), month=int(p[1].split("-")[1]), \
                                                  year=int(p[1].split("-")[0]), value=float(p[3])))
schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.createOrReplaceTempView("tempReadings")

# En join mellan datafilerna?

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
h_distance = 250000 # Distance to station is important
h_date = 25
h_time = 5
a = 58.4274
b = 14.826
date = "2013-07-04"
h = ("04:00:00", "06:00:00", "08:00:00", "10:00:00", "12:00:00", "14:00:00",\
     "16:00:00", "18:00:00", "20:00:00", "22:00:00", "00:00:00")

# Your code here
# Exclude all dates after 2013-07-04
schemaTempReadings = schemaTempReadings.filter("year <= 2014 AND month <= 07 AND day <= 04")

# Gaussian kernel
def kernel(u):
    return(exp(-(abs(u)**2)))

# Distance
# kernel(dateDiff/h_date)

# Date
# dateDiff <- unclass((as.POSIXct(date) - as.POSIXct(st[i,]$date)))
# dateDiff <- dateDiff %% 365
# if (dateDiff > 365/2) {
#    dateDiff <- 365 - dateDiff
# }

# Time
# if (timeDiff > 12) {
#   timeDiff <- timeDiff - 12
# }

# temp[i] <- sum((distanceKernel + dateKernel + timeKernel)*allTemp)/sum(distanceKernel + dateKernel + timeKernel)
