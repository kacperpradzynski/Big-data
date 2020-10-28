import argparse


def main(args):
    print("Hello World!")


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--file", type=str, default="3a.txt", help="Input data file")
    ap.add_argument("-c", "--centroids", type=str, default="3b.txt", help="Centroids file")
    ap.add_argument("-n", "--numberofclusters", type=int, default=10, help="Number of clusters")
    ap.add_argument("-i", "--iterations", type=int, default=20, help="Number of iterations")
    ap.add_argument("-e", "--euclidean", action='store_true', help="Euclidean distance")
    ap.add_argument("-m", "--manhattan", action='store_true', help="Manhattan distance")
    args = vars(ap.parse_args())
    main(args)
