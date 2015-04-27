#!/usr/bin/env python

import os
from psqlgraph import PsqlGraphDriver
from zug.datamodel.tcga_filename_metadata_sync import sync


def main():
    graph = PsqlGraphDriver(
        os.environ["PG_HOST"],
        os.environ["PG_USER"],
        os.environ["PG_PASS"],
        os.environ["PG_NAME"],
    )
    sync(graph)


if __name__ == "__main__":
    main()
