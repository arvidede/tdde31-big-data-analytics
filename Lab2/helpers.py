from os import listdir, getcwd
from os.path import isfile, join

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

