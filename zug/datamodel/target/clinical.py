import os
import pandas as pd
import xlrd
import requests
import re
from datetime import datetime
from uuid import UUID, uuid5

from cdisutils.log import get_logger
from gdcdatamodel import models


CLINICAL_NAMESPACE = UUID('b27e3043-1c1f-43c6-922f-1127905232b0')


ETHNICITY_MAP = {
    "Hispanic or Latino": "hispanic or latino",
    "Not Hispanic or Latinoispanic or Latino": "not hispanic or latino",
    "Unknown": None,
}


def parse_race(race):
    if race.strip() == "Unknown":
        return "not reported"
    else:
        return race.lower().strip()


def parse_row_into_props(row):
    return {
        "gender": row["Gender"].lower().strip(),
        "race": parse_race(row["Race"]),
        "ethnicity": ETHNICITY_MAP[row["Ethnicity"].strip()],
        "vital_status": row["Vital Status"].lower().strip(),
        "year_of_diagnosis": None,
        "age_at_diagnosis": int(row["Age at diagnosis (days)"]),
        "days_to_death": None,
        "icd_10": None,
    }


class TARGETClinicalSyncer(object):

    def __init__(self, project, url, graph=None, dcc_auth=None):
        """
        I am not sure of a good way to automatically determine the correct
        url for a project so for now you have to pass the url explicitly.
        """
        assert url.startswith("https://target-data.nci.nih.gov/{}/Discovery/".format(project))
        self.project = project
        self.url = url
        version_match = re.search("([0-9]{8})", url)
        if not version_match:
            raise RuntimeError("Could not extract version from url {}".format(url))
        self.version = datetime.strptime(version_match.group(1), "%Y%m%d").toordinal()
        self.graph = graph
        self.dcc_auth = dcc_auth
        self.log = get_logger("target_clinical_sync_{}_{}".format(self.project, os.getpid()))

    def load_df(self):
        self.log.info("downloading clinical xlsx from target dcc")
        resp = requests.get(self.url, auth=self.dcc_auth)
        self.log.info("parsing clinical info into dataframe")
        book = xlrd.open_workbook(file_contents=resp.content)
        sheet_names = [sheet.name for sheet in book.sheets()]
        # the whitespace ("Final ") is not a typo, don't change it
        if "Final " in sheet_names:
            SHEET = "Final "
        elif "Sheet1" in sheet_names:
            SHEET = "Sheet1"
        return pd.read_excel(book, engine="xlrd", sheetname=SHEET)

    def create_edge(self, label, src, dst):
        maybe_edge = self.graph.edge_lookup(
            label=label,
            src_id=src.node_id,
            dst_id=dst.node_id,
        ).scalar()
        if not maybe_edge:
            self.graph.edge_insert(self.graph.get_PsqlEdge(
                label=label,
                src_id=src.node_id,
                dst_id=dst.node_id,
                src_label=src.label,
                dst_label=dst.label,
            ))

    def insert(self, df):
        self.log.info("loading clinical info into graph")
        with self.graph.session_scope():
            self.log.info("looking up the node corresponding to %s", self.url)
            clinical_file = self.graph.nodes()\
                                      .labels("file")\
                                      .sysan({"source": "target_dcc",
                                              "url": self.url}).one()
            self.log.info("found clinical file %s as %s", self.url, clinical_file)
            for _, row in df.iterrows():
                # the .strip is necessary because sometimes there is a
                # space after the name, e.g. 'TARGET-50-PAEAFB '
                participant_barcode = row["TARGET Patient USI"].strip()
                self.log.info("looking up participant %s", participant_barcode)
                participant = self.graph.nodes()\
                                        .labels("participant")\
                                        .props({"submitter_id": participant_barcode}).scalar()
                if not participant:
                    self.log.warning("couldn't find participant %s, not inserting clinical data", participant_barcode)
                    continue
                self.log.info("found participant %s as %s, inserting clinical info", participant_barcode, participant)
                clinical = self.graph.node_merge(
                    node_id=str(uuid5(CLINICAL_NAMESPACE, participant_barcode.encode('ascii'))),
                    label="clinical",
                    properties=parse_row_into_props(row),
                    system_annotations={
                        "url": self.url,
                        "version": self.version
                    }
                )
                self.log.info("inserted clinical info as %s, tieing to participant", clinical)
                self.create_edge("describes", clinical, participant)
                self.create_edge("describes", clinical_file, participant)

    def sync(self):
        df = self.load_df()
        self.insert(df)
