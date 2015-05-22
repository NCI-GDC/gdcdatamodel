#!/usr/bin/env python

from argparse import ArgumentParser
from zug.backup import DataBackup


def main():
    parser = ArgumentParser()
    parser.add_argument("--debug", default=False, type=bool)
    parser.add_argument("--file-id", type=str, help="force a specific file id")
    parser.add_argument("--report-file", type=str,
                        help='filename to report to for benchmarking')
    parser.add_argument('--driver', type=str,
                        default='primary_backup')
    parser.add_argument('--constant', type=bool,
                        default=False)

    args = parser.parse_args()
    syncer = DataBackup(
        file_id=args.file_id,
        debug=args.debug,
        reportfile=args.report_file,
        driver=args.driver,
        constant=args.constant
    )
    syncer.backup()


if __name__ == "__main__":
    main()
