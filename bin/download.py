#!/usr/bin/env python

import argparse
from zug.downloaders import Downloader


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=str,
                        help='source to download from')
    parser.add_argument('--analysis-id', type=str,
                        help='analysis id to download')
    args = parser.parse_args()
    assert args.source, "--source must be specified"
    downloader = Downloader(
        source=args.source,
        analysis_id=args.analysis_id,
    )
    downloader.go()

if __name__ == "__main__":
    main()
