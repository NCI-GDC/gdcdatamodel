#!/usr/bin/env python

from zug.datamodel.maf_reconciler import MAFReconciler


def main():
    recon = MAFReconciler()
    recon.reconcile_all()

if __name__ == "__main__":
    main()
