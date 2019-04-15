import pandas as pd
import time


def init():
    data = pd.read_csv('/nfshome/hadoop_examples/shared_data/temperatures-big.csv',
        names=['station_number', 'date', 'time', 'temperature', 'quality'])
    data['year'] = data['date'].apply(lambda x: x.strftime('%Y'))
    data = data.drop(['date', 'station_number', 'time', 'quality'])
    return data


def reduce_by_key(data):
    return data.groupBy(data.year).max()


def filter_by_year(data, start_year=1950, end_year=2014):
    return data[(int(data.year) >= start_year and int(data.year) <= end_year)]


def main():
    start_time = time.time()

    data = init()
    filtered_data = filter_by_year(data)
    elapsed_time = time.time() - start_time
    filtered_data.to_csv(index=False)
    print(elapsed_time)


main()
