#!/usr/bin/env python

from os import environ

from psqlgraph import PsqlGraphDriver
from signpostclient import SignpostClient

from zug.datalocator import DataLocator

from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver


S3 = get_driver(Provider.S3)


def main():
    s3client = S3(environ["ZUGS_S3_ACCESS_KEY"],
                  secret=environ["ZUGS_S3_SECRET_KEY"],
                  host=environ["ZUGS_S3_HOST"], secure=False)
    graph = PsqlGraphDriver(environ["ZUGS_PG_HOST"], environ["ZUGS_PG_USER"],
                            environ["ZUGS_PG_PASS"], environ["ZUGS_PG_NAME"])
    signpost = SignpostClient("http://signpost.service.consul", version="v0")
    locator = DataLocator(
        storage_client=s3client,
        graph=graph,
        signpost_client=signpost
    )
    locator.sync("target_cghub_protected")
    locator.sync("tcga_cghub_protected")


if __name__ == "__main__":
    main()
