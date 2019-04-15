import time
import csv

def init():
    with open('/nfshome/hadoop_examples/shared_data/temperatures-big.csv') as data:
        reader = csv.reader(data)
        for row in reader:
            print(row)

    return []


def reduce_by_key(data):
    return []


def filter_by_year(data, start_year=1950, end_year=2014):
    return []


def main():
    start_time = time.time()

    data = init()
    filtered_data = filter_by_year(data)
    elapsed_time = time.time() - start_time

    print(elapsed_time)


main()
