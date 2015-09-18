import os
import sys
import pandas as pd
import xlrd
import requests
import re
from datetime import datetime
from uuid import UUID, uuid5
from bs4 import BeautifulSoup

from cdisutils.log import get_logger
from gdcdatamodel.models import File, Case


CLINICAL_NAMESPACE = UUID('b27e3043-1c1f-43c6-922f-1127905232b0')


ETHNICITY_MAP = {
    "Hispanic or Latino": "hispanic or latino",
    "Not Hispanic or Latinoispanic or Latino": "not hispanic or latino",
    "Not Hispanic or Latino": "not hispanic or latino",
    "Unknown": None,
    "Not Reported": None,
}

AGE_TITLE_STRINGS = [ 
        "Age at diagnosis (days)", 
        "Age at Diagnosis in Days"
]

BARCODE_TITLE_STRINGS = [
    "TARGET Patient USI",
    "TARGET USI"
]

VITAL_STATUS_MAP = {
    "Alive": "alive",
    "Dead": "dead",
    "Unknown": None,
    "Lost to Follow-up": "lost to follow-up"
}

BASE_URL = "https://target-data.nci.nih.gov"

ACCESS_LEVELS = [
    "Controlled",
    "Public"
]

PROJECTS_TO_SYNC = { 
    # "ALL-P1",
    # "ALL-P2",
    #"ALL/Phase_I" : "/Discovery/clinical/harmonized/",  # temp
    #"ALL/Phase_II" : "/Discovery/clinical/harmonized/", # temp
    "AML" : "Discovery/clinical/harmonized/",
    #"AML-IF" : "/Discovery/clinical/",                  # temp
    #"CCSK" : "/Discovery/clinical/harmonized/",         # temp
    "NBL" : "Discovery/clinical/harmonized/",
    #"OS" : "/Discovery/clinical/",                      # temp
    #"RT" : "/Discovery/clinical/harmonized/",           # temp
    "WT" : "Discovery/clinical/harmonized/"
}

ROW_CLASSES = [ "even", "odd" ]

def parse_race(race):
    if race.strip() == "Unknown":
        return "not reported"
    else:
        return race.lower().strip()


def parse_vital_status(vital_status):
    if vital_status.strip() in VITAL_STATUS_MAP:
        return VITAL_STATUS_MAP[vital_status.strip()]
    else:
        raise RuntimeError("Unknown vital status:", vital_status)

def parse_row_into_props(row):

    for entry in AGE_TITLE_STRINGS:
        if entry in row:
            age_row_string = entry

    return {
        "gender": row["Gender"].lower().strip(),
        "race": parse_race(row["Race"]),
        "ethnicity": ETHNICITY_MAP[row["Ethnicity"].strip()],
        "vital_status": parse_vital_status(row["Vital Status"]),
        "year_of_diagnosis": None,
        "age_at_diagnosis": int(row[age_row_string]),
        "days_to_death": None,
        "icd_10": None,
    }


def match_date(string_to_check):

        version = None
        version_match = re.search("([0-9]{8})", string_to_check)
        if version_match:
            version = datetime.strptime(version_match.group(1), "%Y%m%d").toordinal()

        if not version:
            version_match = re.search("([1-9]|1[012])[_ /.]([1-9]|[12][0-9]|3[01])[_ /.](19|20)\d\d",
                string_to_check
            )
            if version_match:
                version = datetime.strptime(version_match.group(0), "%m_%d_%Y").toordinal()

        return version

def process_tree(url, dcc_user, dcc_pass):
    """Walk the given url and recursively find all the spreadsheet links."""
    url_list = []
    r = requests.get(url, auth=(dcc_user, dcc_pass), verify=False)
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, "lxml")
        file_table = soup.find('table', attrs={'id':'indexlist'})
        rows = file_table.find_all('tr')
        for row in rows:
            if row['class'][0] in ROW_CLASSES:
                image = row.find('img')
                if image['alt'].find("DIR") == -1:
                    dir_data = {}
                    dir_data['dir_name'] = row.find('td', class_="indexcolname").get_text().strip()
                    link = row.find('a')
                    if (link['href'].find("xlsx") != -1) and (link['href'].find("Clinical") != -1):
                        dir_data['url'] = url + link['href']
                        url_list.append(dir_data)

    return url_list

def find_clinical(args):
    spreadsheet_urls = {}
    for project in args.projects:
        for access_level in ACCESS_LEVELS:
            url = "/".join([BASE_URL, access_level, project, PROJECTS_TO_SYNC[project]])
            spreadsheets = process_tree(url, args.dcc_user, args.dcc_pass)
            spreadsheet_urls[project] = spreadsheets
    
    return spreadsheet_urls

class TARGETClinicalSyncer(object):

    def __init__(self, project, url, graph=None, dcc_auth=None):
        """
        I am not sure of a good way to automatically determine the correct
        url for a project so for now you have to pass the url explicitly.
        """

        url_verified = False
        for level in ACCESS_LEVELS:
            url_str = "/".join([BASE_URL, level, project, "Discovery"])
            if url.startswith(url_str):
                url_verified = True
                break

        #url_str = "%s%s/Discovery" % (base_url, project)
        assert url_verified
        #assert url.startswith(url_str)
        #assert url.startswith("https://target-data.nci.nih.gov/{}/Discovery/".format(project))
        self.project = project
        self.url = url
        self.version = match_date(url)
        if not self.version:
            raise RuntimeError("Could not extract version from url {}".format(url))
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
        elif "Clinical Data" in sheet_names:
            SHEET = "Clinical Data"
        else:
            error_str = "Unknown sheet names:", sheet_names
            self.log.error(error_str)
            raise RuntimeError(error_str)

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
            clinical_file = self.graph.nodes(File)\
                                      .sysan({"source": "target_dcc",
                                              "url": self.url}).one()
            self.log.info("found clinical file %s as %s", self.url, clinical_file)
            row_count = 0
            for _, row in df.iterrows():
                # the .strip is necessary because sometimes there is a
                # space after the name, e.g. 'TARGET-50-PAEAFB '
                case = None
                for column_title in BARCODE_TITLE_STRINGS:
                    case_barcode = None
                    if column_title in row:
                        if (type(row[column_title]) == str) or (type(row[column_title]) == unicode):
                            case_barcode = row[column_title].strip()
                            break
                        else:
                            self.log.info("Unrecognized type: %s at %d in %s", 
                                str(type(row[column_title])), row_count, column_title
                            )
                if case_barcode:
                    self.log.info("looking up case %s", case_barcode)
                    case = self.graph.nodes(Case)\
                           .props({"submitter_id": case_barcode}).scalar()
                if not case:
                    self.log.warning("couldn't find case %s, not inserting clinical data", case_barcode)
                    continue
                self.log.info("found case %s as %s, inserting clinical info", case_barcode, case)
                clinical = self.graph.node_merge(
                    node_id=str(uuid5(CLINICAL_NAMESPACE, case_barcode.encode('ascii'))),
                    label="clinical",
                    properties=parse_row_into_props(row),
                    system_annotations={
                        "url": self.url,
                        "version": self.version
                    }
                )
                self.log.info("inserted clinical info as %s, tieing to case", clinical)
                self.create_edge("describes", clinical, case)
                self.create_edge("describes", clinical_file, case)
                row_count += 1

    def sync(self):
        df = self.load_df()
        self.insert(df)
