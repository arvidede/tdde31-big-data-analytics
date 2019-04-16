import json

def formatAndSaveDict(dict):
    f = open('results/results_1_2_a.txt', 'w')
    for keys, count in f.items():
        f.write(str(keys[0]) + ',' + str(keys[1]) + ',' + str(count) + '\n')
    f.close()


def main():
    f = open('./Lab1/lab1_2_a/part-00015')
    print(f)
    for row in f:
        print(row)


main()

