import argparse


def main(args):
    print("Hello World!")


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--file", type=str, default="4.txt", help="Input data file")
    args = vars(ap.parse_args())
    main(args)