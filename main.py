#!/usr/bin/python3

import simplefft
from server import Server

import argparse
import logging

defaults = {
    'host': '0.0.0.0',
    'port': 9002
}


def main(args):

    server = Server(args.host, args.port, simplefft.process)
    server.serve()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='I should be able to write this.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-p', '--port', type=int, help='Port')
    parser.add_argument('-H', '--host', type=str, help='Host')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output.')
    parser.add_argument('-l', '--logfile', type=str, help='Path to log file')

    parser.set_defaults(**defaults)

    args = parser.parse_args()

    if args.logfile:
        print("Logging to file: ", args.logfile)
        logging.basicConfig(filename=args.logfile, format='%(asctime)s - %(message)s', level=logging.WARNING)
    else:
        print("No logfile provided")
        logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.WARNING)

    if args.verbose:
        logging.root.setLevel(logging.DEBUG)

    main(args)
