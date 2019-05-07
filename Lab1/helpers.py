import json
from datetime import datetime


def formatAndSaveDict(dict):
    f = open('results/results_1_2_a.txt', 'w')
    for keys, count in f.items():
        f.write(str(keys[0]) + ',' + str(keys[1]) + ',' + str(count) + '\n')
    f.close()


def formatAndSave(file):
    f = open('results/lab1_5.txt', 'w')

    data = file.read().split('\n')
    data.sort(key=lambda date: datetime.strptime(date[2:9], '%Y-%m'))

    for row in data:
        f.write(row[2:9] + ':' + row[(row.find(',')+1):row.rfind(')')] + '\n')

def main():
    f = open('./results/lab1_5/part-00000')
    formatAndSave(f)


# main()


def testMonthExtraction():
    f = open('./data/temperature-readings.csv')
    data = f.read().split('\n')
    del data[-1]
    line = data[0].split(';')
    print(line[1][5:7])


# testMonthExtraction()


def formatAndSaveTaskTwo():
    file = open('./results/min_temperature/part-00000')
    f = open('results/lab1_1_min.txt', 'w')

    data = file.read().split('\n')
    del data[-1]

    for row in data:
        f.write(row[4:(row.find(',')-1)] + ', ' + row[(row.find(',')+4):(row.find(',')+8)] + ': ' + row[(row.rfind(',')+1):(row.rfind(')'))] + '\n')



formatAndSaveTaskTwo()
