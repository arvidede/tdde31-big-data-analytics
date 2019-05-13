# BDA3 - Machine learning with spark
from future import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext

stations = sc.textFile("../data/stations.csv")
temps = sc.textFile("../data/temperature-readings.csv")

sc = SparkContext(appName="lab_kernel")
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
