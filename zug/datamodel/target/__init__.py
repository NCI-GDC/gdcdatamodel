import pandas as pd
import os

PROJECTS = ["ALL-P1", "ALL-P2", "ALL", "AML", "AML-IF", "CCSK", "NBL", "OS", "RT", "WT"]

BARCODE_DF = pd.read_table(os.path.join(os.path.dirname(__file__), "barcodes.tsv"),
                           low_memory=False)


def barcode_to_aliquot_id_dict():
    return {row["barcode"]: row["aliquot_id"] for _, row in BARCODE_DF.iterrows()
            if row["redaction"] == "\\N"}
