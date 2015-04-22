#!/usr/bin/env python

from zug.datamodel.tcga_magetab_sync import TCGAMAGETABSyncer


def main():
    syncer = TCGAMAGETABSyncer()
    syncer.sync()
    # TODO attempt to biospecemin sync if we dont have an MAGETABs to
    # do

if __name__ == "__main__":
    main()
