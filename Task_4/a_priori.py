import argparse
import itertools
import copy

from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)


def create_singles(row):
    connections = []
    for element in row:
        possible_res = copy.deepcopy(row)
        possible_res.remove(element)
        connections.append((element, possible_res))
    return connections


def create_pairs(row):
    connections = []
    for pairs in itertools.combinations(row, 2):
        key = tuple(sorted((pairs[0], pairs[1])))
        possible_res = copy.deepcopy(row)
        possible_res.remove(pairs[0])
        possible_res.remove(pairs[1])
        connections.append((key, possible_res))
    return connections


def create_trios(row):
    connections = []
    for pairs in itertools.combinations(row, 3):
        key = tuple(sorted((pairs[0], pairs[1])))
        possible_res = copy.deepcopy(row)
        possible_res.remove(pairs[0])
        possible_res.remove(pairs[1])
        possible_res.remove(pairs[2])
        connections.append((key, possible_res))
    return connections


def main(args):
    print("Hello World!")
    data_file = args['file']

    sessions = sc.textFile(data_file).map(
        lambda x: [str(s) for s in x.split()])
    print(sessions.take(5))

    pairs = sessions.flatMap(create_pairs)
    pairs.cache()
    print(pairs.take(10))

    result = pairs.map(lambda pair: (pair[0], 1)).reduceByKey(lambda a,b: a+b).filter(lambda pair: pair[1] >= 5)
    print(result.take(10))
    print(result.count())


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--file", type=str,
                    default="test.txt", help="Input data file")
    args = vars(ap.parse_args())
    main(args)
