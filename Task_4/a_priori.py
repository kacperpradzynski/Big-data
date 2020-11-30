import argparse
import itertools
import copy

from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)


def create_singles(row):
    connections = []
    for element in row:
        connections.append((element, 1))
    return connections


def create_pairs(row):
    connections = []
    for pairs in itertools.combinations(row, 2):
        key = tuple(sorted((pairs[0], pairs[1])))
        connections.append((key, 1))
    return connections


def create_trios(row):
    connections = []
    for pairs in itertools.combinations(row, 3):
        key = tuple(sorted((pairs[0], pairs[1], pairs[2])))
        connections.append((key, 1))
    return connections


def confidence(i, j):
    return j/i


def confidence_doubles(pair, singles_dict):
    confidences = []
    confidences.append(((pair[0][0], pair[0][1]), confidence(singles_dict[pair[0][0]], pair[1]))) 
    confidences.append(((pair[0][1], pair[0][0]), confidence(singles_dict[pair[0][1]], pair[1])))
    return confidences


def confidence_triples(pair, doubles_dict):
    confidences = []

    ikeys = itertools.combinations(pair[0], 2)
    for ikey in ikeys:
            if ikey in doubles_dict.keys():
                confidences.append((tuple(list(ikey) + list(set(pair[0]) - set(ikey))), confidence(doubles_dict[ikey], pair[1])))
    return confidences


def main(args):
    data_file = args['file']
    limit = 100

    sessions = sc.textFile(data_file).map(
        lambda x: [str(s) for s in x.split()])

    sessions.cache()

    print("Generate frequent singles")
    singles = sessions.flatMap(create_singles).reduceByKey(lambda a, b: a+b).filter(lambda pair: pair[1] >= limit)
    
    print("Generate frequent doubles")
    doubles = sessions.flatMap(create_pairs).reduceByKey(lambda a, b: a+b).filter(lambda pair: pair[1] >= limit)

    singles_dict = singles.collectAsMap()
    double_confidence = doubles.flatMap(lambda pair: confidence_doubles(pair, singles_dict)).sortBy(lambda x: (-x[1], x[0]))

    print("Generate frequent triples")
    triples = sessions.flatMap(create_trios).reduceByKey(lambda a, b: a+b).filter(lambda pair: pair[1] >= limit)

    doubles_dict = doubles.collectAsMap()
    triple_confidence = triples.flatMap(lambda pair: confidence_triples(pair, doubles_dict)).sortBy(lambda x: (-x[1], sorted(x[0])))

    with open("output_double.txt", "w", encoding="utf-8") as fo:
        for pair, confidence in double_confidence.collect():
            fo.write(
                "".join(["%s [%s] %s" % (pair[0], pair[1], confidence)]) + "\n")

    with open("output_triple.txt", "w", encoding="utf-8") as fo:
        for pair, confidence in triple_confidence.collect():
            fo.write(
                "".join(["%s %s [%s] %s" % (pair[0], pair[1], pair[2], confidence)]) + "\n")


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--file", type=str,
                    default="4.txt", help="Input data file")
    args = vars(ap.parse_args())
    main(args)
