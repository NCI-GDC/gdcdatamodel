#!/usr/bin/env python

import argparse
from zug.gdc_elasticsearch import GDCElasticsearch


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-roll', type="store_false",
                        help='if passed, do not roll the alias and delete old indices')
    args = parser.parse_args()
    gdc_es = GDCElasticsearch(roll_alias=args.roll_alias)
    gdc_es.go()
