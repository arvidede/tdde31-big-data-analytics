from os import listdir, getcwd
from os.path import isfile, join
from datetime import datetime, timedelta
from pyspark import SparkContext

def collectAndSortPartitions():
    myPath = getcwd() + '/results/lab2_1_max/'
    onlyFiles = [f for f in listdir(myPath) if isfile(join(myPath, f)) and f[-3:] == 'csv']

    f = open('results/lab2_1_max.txt', 'w')
    data = []

    for filePath in onlyFiles:
        rf = open(myPath + filePath)
        for line in rf:
            data.append(line)

    data.sort(key = lambda x: x[0:5])

    for row in data:
        f.write(row)

    f.close()


def sortByValue():
    fr = open('results/lab2_6.txt')
    fw = open('results/6.txt', 'w')

    data = []
    for line in fr:
        data.append(line)

    data.sort(key = lambda x: (float(x.split(',')[-1]), float(x.split(',')[0])), reverse = True)

    for row in data:
        fw.write(row)

    fr.close()
    fw.close()


def testParseDate():

    # Misc input and setup
    sc = SparkContext(appName = "lab1_6")

    # Local
    station_file = sc.textFile("./data/stations.csv")

    station_lines = station_file.map(lambda line: line.split(";"))

    # Station number as list
    stations = station_lines.map(lambda x: (x[0], (float(x[3]), float(x[4]))))
    stations = dict(stations.collect())
    print('lon', stations['99330'][0], 'lat', stations['99330'][1])


testParseDate()
