import argparse
import numpy as np

from math import sqrt
from statistics import mean
from datetime import datetime
from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)


def calculate_distance(x, y):
    if manhattan_distance:
        result = [abs(a-b) for a, b in zip(x, y)]
        result = sum(result)
    else:
        result = [(a-b)**2 for a, b in zip(x, y)]
        result = sqrt(sum(result))
    return result


def calculate_cost(pair):
    centroid = centroids_list[pair[0]]
    row = pair[1]
    if manhattan_distance:
        result = calculate_distance(row, centroid)
    else:
        result = calculate_distance(row, centroid)**2
    return result


def assign_to_cluster(row):
    distances = []
    for i in range(len(centroids_list)):
        distances.append(calculate_distance(row, centroids_list[i]))
    closest_centroid = distances.index(min(distances))
    return (closest_centroid, row)


def calculate_centroids(cluster):
    global centroids_list
    centroid_idx = cluster[0]
    points = list(cluster[1])
    new_centroid = list(map(mean, zip(*points)))  # sum(points) / len(points)
    return (centroid_idx, new_centroid)


def main(args):
    dataset_file = args['file']
    centroids_file = args['centroids']
    max_iterations = args['iterations']

    global centroids_list
    global manhattan_distance

    if args['manhattan']:
        manhattan_distance = True
    else:
        manhattan_distance = False

    dataset = sc.textFile(dataset_file).map(
        lambda x: [float(s) for s in x.split()])
    initial_centroids = sc.textFile(centroids_file).map(
        lambda x: [float(s) for s in x.split()])

    centroids_list = initial_centroids.collect()

    iteration_costs = []

    for i in range(max_iterations):
        assigned_points = dataset.map(assign_to_cluster).cache()

        costs = assigned_points.map(calculate_cost)
        iteration_cost = costs.reduce(lambda x, y: x+y)
        iteration_costs.append(iteration_cost)

        clusters = assigned_points.groupByKey()
        new_centroids = clusters.map(calculate_centroids)
        centroids_list = new_centroids.map(lambda x: x[1]).collect()

    assigned_points = dataset.map(assign_to_cluster).cache()

    costs = assigned_points.map(calculate_cost)
    iteration_cost = costs.reduce(lambda x, y: x+y)
    iteration_costs.append(iteration_cost)

    # print(centroids_list)
    print(iteration_costs)
    with open("centroids.txt", "w", encoding="utf-8") as fo:
        for centroid in centroids_list:
            fo.writelines(["%s," % round(item, 4) for item in centroid])

    with open("costs.txt", "w", encoding="utf-8") as fo:
        fo.writelines(["%s\n" % item for item in iteration_costs])


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--file", type=str,
                    default="3a.txt", help="Input data file")
    ap.add_argument("-c", "--centroids", type=str,
                    default="3b.txt", help="Centroids file")    
    ap.add_argument("-i", "--iterations", type=int,
                    default=20, help="Number of iterations")   
    ap.add_argument("-m", "--manhattan", action='store_true',
                    help="Manhattan distance (Default Euclidean)")
    args = vars(ap.parse_args())
    main(args)
