import argparse


def main(args):
    print(args['filename'])


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--filename", type=str, default="311_Service_Requests_from_2010_to_Present.csv", help="filename")
    args = vars(ap.parse_args())
    main(args)
