import pandas as pd
from sample_matrices import *


BARCODE_DF = pd.read_table(os.path.join(os.path.dirname(__file__), "barcodes.tsv"),
                           low_memory=False)


def barcode_to_aliquot_id_dict():
    return {row["barcode"]: row["aliquot_id"] for _, row in BARCODE_DF.iterrows()
            if row["redaction"] == "\\N"}
