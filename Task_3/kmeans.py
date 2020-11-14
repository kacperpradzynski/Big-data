import argparse
import numpy as np

from math import sqrt
from datetime import datetime
from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)


def calculate_distance(x, y):
    global manhattan_distance
    if manhattan_distance:
        result = [abs(a-b) for a, b in zip(x, y)]
        result = sum(result)
    else:
        result = [(a-b)**2 for a, b in zip(x, y)]
        result = sqrt(sum(result))
    return result


def calculate_cost(pair):
    global centroids_list
    global manhattan_distance
    row = pair[1]
    centroid = centroids_list[pair[0]]
    if manhattan_distance:
        result = calculate_distance(row, centroid)
    else:
        result = calculate_distance(row, centroid)**2
    return result


def assign_to_cluster(row):
    global centroids_list
    distances = []
    for i in range(len(centroids_list)):
        distances.append(calculate_distance(row, centroids_list[i]))
    # print(min(distances))
    closest_centroid = distances.index(min(distances))
    # print(distances.index(min(distances)))
    return (closest_centroid, row)


def calculate_centroids(cluster):
    global centroids_list
    centroid_idx = cluster[0]
    points = list(cluster[1])
    # print("Points:")
    # print(points)
    # points = np.array(list(cluster[1]))
    # print(points)
    sum=[]
    for point in points:
        sum += point
    new_centroid = [x / len(points) for x in sum]
    # new_centroid = np.average(points, axis=0)    
    # print(centroids_list[centroid_idx])]
    # print("New centroid")
    # print(new_centroid)
    # centroids_list[centroid_idx] = new_centroid
    # print(centroids_list[centroid_idx])
    return (centroid_idx, new_centroid)


def main(args):
    dataset_file = args['file']
    centroids_file = args['centroids']
    max_iterations = 3
    # max_iterations = args['iterations']
    # clusters = args['numberofclusters']

    global centroids_list
    global manhattan_distance

    if args['manhattan']:
        manhattan_distance = True
    else:
        manhattan_distance = False

    # print(manhattan_distance)

    dataset = sc.textFile(dataset_file).map(
        lambda x: [float(s) for s in x.split()])
    initial_centroids = sc.textFile(centroids_file).map(
        lambda x: [float(s) for s in x.split()])

    centroids_list = initial_centroids.collect()

    iteration_costs = []

    for i in range(max_iterations):
        assigned_points = dataset.map(assign_to_cluster)
        costs = assigned_points.map(calculate_cost)
        # print(costs.take(100))

        iteration_cost = costs.reduce(lambda x, y: x+y)
        iteration_costs.append(iteration_cost)
        # print(iteration_cost)
        
        # print(len(assigned_points.take(1000)))  
        clusters = assigned_points.groupByKey()
        # print(clusters.take(1000))      
        # print(len(clusters.take(1000)))  

        new_centroids = clusters.map(calculate_centroids)
        # print(new_centroids.take(15))
        # print(len(new_centroids.take(1000)))

        new_cent = new_centroids.collect()
        for (x,y) in new_cent:
            centroids_list[x] = y

    # print(len(centroids_list))
    # print(centroids_list)
    print(iteration_costs)
    with open("centroids.txt", "w", encoding="utf-8") as fo:
        for centroid in centroids_list:
            fo.writelines(["%s," % round(item, 4)  for item in centroid])

    with open("costs.txt", "w", encoding="utf-8") as fo:
        fo.writelines(["%s\n" % item  for item in iteration_costs])


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--file", type=str,
                    default="3a.txt", help="Input data file")
    ap.add_argument("-c", "--centroids", type=str,
                    default="3b.txt", help="Centroids file")
    ap.add_argument("-n", "--numberofclusters", type=int,
                    default=10, help="Number of clusters")
    ap.add_argument("-i", "--iterations", type=int,
                    default=20, help="Number of iterations")
    ap.add_argument("-e", "--euclidean", action='store_true',
                    help="Euclidean distance")
    ap.add_argument("-m", "--manhattan", action='store_true',
                    help="Manhattan distance")
    args = vars(ap.parse_args())
    main(args)
