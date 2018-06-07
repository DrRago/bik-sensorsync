import csv
import os
import time
from itertools import chain


def split_file(path, pattern, size):
    with open(path) as f:
        for index, line in enumerate(f, start=1):
            with open(pattern.format(index), 'w') as out:
                n = 0
                for line in chain([line], f):
                    out.write(line)
                    n += len(line)
                    if n >= size:
                        break


if __name__ == "__main__":
    dir = "C:/Users/Leonhard.Gahr/Documents/KIPro/2018-05-24-14-15-36-14_daten/udp_x102/2018_05_24/"
    file_name = "kipro-analyser-data_2018-5-24_13-53-13.601.csv"
    for file in os.listdir(dir):
        if file != file_name:
            os.remove(dir + file)
    split_file(
        dir + file_name,
        dir + "kipro-analyser-data_{0:03d}.csv",
        104857600/2)
    start = time.time()
    with open(dir + "kipro-analyser-data_001.csv") as file:
        csv_reader = csv.reader(file, delimiter=';')
        for row in csv_reader:
            pass
    print(time.time() - start)
