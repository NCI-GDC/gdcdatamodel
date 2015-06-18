#!/usr/bin/env python

from argparse import ArgumentParser
from zug.backup import DataBackup, get_statistics


def main():
    parser = ArgumentParser()
    parser.add_argument("--debug", default=False, type=bool)
    parser.add_argument('--driver', type=str,
                        default='primary_backup')
    parser.add_argument("--stats", default=False)
    parser.add_argument("--summary", default=False)
    parser.add_argument("--output", default='')
    parser.add_argument("--bucket", default='gdc_backup')
    args = parser.parse_args()
    if args.stats:
        get_statistics(summary=args.summary, output=args.output,
                       driver=args.driver, bucket=args.bucket)
    else:
        syncer = DataBackup(
            debug=args.debug,
            driver=args.driver,
        )
        syncer.backup()


if __name__ == "__main__":
    main()
