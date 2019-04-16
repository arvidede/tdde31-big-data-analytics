import time
import csv

def init(years):
    with open('/nfshome/hadoop_examples/shared_data/temperatures-big.csv') as data:
    # with open('./test.txt') as data:
        reader = csv.reader(data)
        for row in reader:
            lines = row[0].split(";")
            years = filter_by_year(lines, years)
    return years


def filter_by_year(row, years, start_year=1950, end_year=2014):
    year = int(row[1][0:4])
    temp = float(row[3])
    if year >= 1950 and year <= 2014:
        if not year in years:
            years[year] = temp
        elif years[year] < temp:
            years[year] = temp
    return years


def write_to_file(data):
    f = open("results.txt", "w")
    for row in data:
        f.write(str(row) + ' : ' + str(data[row]) + '\n')
    f.close()

def main():
    start_time = time.time()
    years = {}
    years = init(years)
    elapsed_time = time.time() - start_time
    write_to_file(years)
    print('Elapsed time:', elapsed_time)
    print('Max temperatures:', years)


main()
