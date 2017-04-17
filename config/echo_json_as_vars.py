""""This simple script parses a JSON database credential file and prints each
entry as a PG* shell variable assignment for the psql command line program."""
import sys
import json
import pipes
import argparse


def echo_json_as_vars(filename):
    with open(filename) as f:
        config = json.load(f)
        for k,v in config.items():
            print('PG' + k.upper() + "=" + pipes.quote(str(v)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", type= str, help="JSON file to use")
    args = parser.parse_args()
    echo_json_as_vars(args.filename)
