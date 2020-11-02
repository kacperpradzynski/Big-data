import argparse
import itertools

from datetime import datetime
from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)


def split_data(row):
    split = row.split('\t')
    user = int(split[0])
    friends = []
    if len(split) > 1 and split[1] != '':
        friends = list(map(lambda x: int(x), split[1].split(',')))
    return user, friends


def connect_friends(user_friends):
    connections = []
    user = user_friends[0]
    friends = user_friends[1]
    for friend in friends:
        key = tuple(sorted((user, friend)))
        connections.append((key, 0))
    for pairs in itertools.combinations(friends, 2):
        key = tuple(sorted((pairs[0], pairs[1])))
        connections.append((key, 1))
    return connections


def pair_recomendations(mutual_friends):
    friends = mutual_friends[0]
    mutual_friends_count = mutual_friends[1]
    pair1 = (friends[0], (friends[1], mutual_friends_count))
    pair2 = (friends[1], (friends[0], mutual_friends_count))
    return [pair1, pair2]


def sort_recommendations(recs, number_of_recomendations):
    recs.sort(key=lambda x: (-int(x[1]), int(x[0])))
    return list(map(lambda x: x[0], recs))[:number_of_recomendations]


def main(args):
    filename = args['file']
    number_of_recomendations = int(args['numberofusers'])
    start_time = datetime.now()

    users = sc.textFile(filename)
    users_friends = users.map(split_data)
    users_connections = users_friends.flatMap(connect_friends)
    mutual_friends = users_connections.groupByKey().filter(
        lambda pair: 0 not in pair[1]).map(lambda pair: (pair[0], sum(pair[1])))
    recommendations = mutual_friends.flatMap(
        pair_recomendations)
    users_recommendations = recommendations.groupByKey().map(
        lambda mf: (mf[0], sort_recommendations(list(mf[1]), number_of_recomendations)))
    user_ids_recs = users_recommendations.filter(lambda recs: recs[0] in [
                                                 924, 8941, 8942, 9019, 9020, 9021, 9022, 9990, 9992, 9993]).sortByKey()
    end_time = datetime.now()
    elapsed_time = (end_time - start_time)
    print("Processing time: ", elapsed_time)
    print("Result:", user_ids_recs.take(10))


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--file", type=str,
                    default="2.txt", help="Input data file")
    ap.add_argument("-n", "--numberofusers", type=int,
                    default=10, help="Number of users")
    args = vars(ap.parse_args())
    main(args)
