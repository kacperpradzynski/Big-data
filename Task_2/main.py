import argparse
import itertools

from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)


def divide_data(row):
    split = row.split('\t')
    user = int(split[0])
    friends = []
    if len(split) > 1 and split[1] != '':
        friends = list(map(lambda x: int(x), split[1].split(',')))
    return user, friends


def user_connected_friends(friendships):
    user_connections = []
    for friend in friendships[1]:
        key = tuple(sorted((friendships[0], friend)))
        user_connections.append((key, 0))
    for pairs in itertools.combinations(friendships[1], 2):
        key = tuple(sorted((pairs[0], pairs[1])))
        user_connections.append((key, 1))
    return user_connections


def user_friend_recommendation_pairs(mutual):
    mutual_friend_pair = mutual[0]
    mutual_friends_count = mutual[1]
    f1 = mutual_friend_pair[0]
    f2 = mutual_friend_pair[1]
    rec1 = (f1, (f2, mutual_friends_count))
    rec2 = (f2, (f1, mutual_friends_count))
    return [rec1, rec2]


def sort_recommendations(recs):
    recs.sort(key=lambda x: (-int(x[1]), int(x[0])))
    return list(map(lambda x: x[0], recs))[:10]


def main(args):
    print("Hello World!")

    users = sc.textFile(args['file'])

    friendship_pairs = users.map(divide_data)
    # print(friendship_pairs.take(10))

    friend_connections = friendship_pairs.flatMap(user_connected_friends)
    # print(friend_connections.take(100))

    mutual_connections = friend_connections.groupByKey().filter(
        lambda pair: 0 not in pair[1]).map(lambda pair: (pair[0], sum(pair[1])))
    # print(mutual_connections.take(100))

    recommendations = mutual_connections.flatMap(
        user_friend_recommendation_pairs)
    # print(recommendations.take(100))

    friend_recommendations = recommendations.groupByKey().map(
        lambda mf: (mf[0], sort_recommendations(list(mf[1]))))
    # print(friend_recommendations.take(100))

    # user_ids_recs = friend_recommendations.filter(lambda recs: recs[0] in [924, 8941, 8942, 9019, 9020, 9021, 9022, 9990, 9992, 9993]).sortByKey()
    user_ids_recs = friend_recommendations.filter(lambda recs: recs[0] in [11]).sortByKey()
    print(user_ids_recs.take(5))

    print("End of the World!")


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--file", type=str,
                    default="test.txt", help="Input data file")
    ap.add_argument("-n", "--numberofusers", type=int,
                    default=10, help="Number of users")
    args = vars(ap.parse_args())
    main(args)
