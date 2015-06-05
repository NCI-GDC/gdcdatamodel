#!/usr/bin/env python

import argparse
from zug.gdc_elasticsearch import GDCElasticsearch


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-roll', action="store_true",
                        help='if passed, do not roll the alias and delete old indices')
    args = parser.parse_args()
    gdc_es = GDCElasticsearch()
    gdc_es.go(roll_alias=not args.no_roll)

if __name__ == "__main__":
    main()
