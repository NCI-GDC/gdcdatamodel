import os, sys
import json
import re
import logging
import settings
import requests
import psycopg2
import StringIO
from psycopg2 import extras
from lxml import etree
from datetime import datetime, tzinfo, timedelta

DEFAULTS = os.path.join(os.path.dirname(__file__),'defaults.json')
logging.basicConfig(level=logging.INFO)

#because python isoformat() isn't compliant without tz, alt is to use pytz
class SimpleUTC(tzinfo):
    def tzname(self):
        return "UTC"
    def utcoffset(self, dt):
        return timedelta(0)

class TCGACGHubImporter:

    def __init__(self, path=DEFAULTS):
        #strict=False because the header string contains tabs
        self.psql_settings = settings.Settings(path, "psql")
        self.cghub_settings = settings.Settings(path, "legacy")

    def _add_metadata_fields(self, json_rep):
        json_rep["import_state"] = "not_started"
        json_rep["metadata_added"] = datetime.utcnow().replace(tzinfo=SimpleUTC()).replace(microsecond=0).isoformat()
        json_rep["import_host"] = None
        json_rep["md5sum_finish"] = None
        json_rep["import_start"] = None
        json_rep["import_finish"] = None
        json_rep["download_start"] = None
        json_rep["download_finish"] = None
        json_rep["md5sum_start"] = None
        json_rep["md5sum_finish"] = None
        json_rep["gdc_did"] = None

    def _psql_sync_archive_metadata(self, json_metadata):
        self._add_metadata_fields(json_metadata)

        logging.info("json_metadata: %s"  % json.dumps(json_metadata, indent=4, sort_keys=True))

        conn = psycopg2.connect(dbname=self.psql_settings["db"], user=self.psql_settings["user"], password=self.psql_settings["passwd"])
        cur = conn.cursor()
        #first check if exists
        #        cur.execute("SELECT * FROM tcga_dcc WHERE doc @> %s", (json_metadata["center_name"],))
        cur.execute("SELECT doc::json FROM tcga_cghub WHERE doc @> %s", [extras.Json({"analysis_id" : json_metadata["analysis_id"]})])

        add = True

        #TODO - actually sync
        if cur.rowcount > 0:
            logging.info("Current analysis already exists")
            add = False

        if add:
            logging.info("Adding json_metadata %s" % json.dumps(json_metadata, indent=4))
            cur.execute("INSERT INTO tcga_cghub (doc) VALUES (%s)", (json.dumps(json_metadata, sort_keys=True),))

        conn.commit()
        cur.close()
        conn.close()

    #TODO - combine with jmiller's parsing
    def _convert_xml2json(self, xml):
        context = etree.iterparse(StringIO.StringIO(xml), events=("start", "end"))
        json_conv = {}
        json_conv["files"] = []
        in_file = False
        file_data = {}

        for action, elem in context:
            if elem.tag == "files":
                continue
            elif elem.tag == "file" and action == "start":
                file_data = {}
                in_file = True
                continue
            elif elem.tag == "file" and action == "end":
                json_conv["files"].append(file_data)
                in_file = False
                continue

            if in_file:
                file_data[elem.tag] = elem.text
            else:
                json_conv[elem.tag] = elem.text

            print("%s: %s" % (action, elem.tag))
            print("value: %s" % (elem.text))

        print(json.dumps(json_conv, indent=4, sort_keys=True))
        return json_conv
        

    def import_all(self):
        conn = psycopg2.connect(dbname=self.cghub_settings["db"], host=self.cghub_settings["host"], port=self.cghub_settings["port"],
                                user=self.cghub_settings["user"], password=self.cghub_settings["passwd"])

        cur = conn.cursor()
        cur.execute("SELECT cghub_result FROM tcga_seq")

        print("rowcount: %d" % cur.rowcount)

        for record in cur:
            result_xml = record[0]
            result_json = self._convert_xml2json(result_xml)
            self._psql_sync_archive_metadata(result_json)
            break


        cur.close()
        conn.close()

        '''
        archive_list = self._get_dcc_latest()

        latest = archive_list.splitlines()
        header = latest[0]
        if header != self.dcc_settings["latest_header"]:
            logging.info(self.dcc_settings["latest_header"])
            logging.error("Latest report did not contain expected header. Contained: \n%s" % header)
            #TODO appropriate exception here
            raise

        archive_metadata = {}

        for line in latest[1:]:
            sline = line.strip().split('\t')
            archive_metadata["archive_name"] = sline[0]
            archive_metadata["date_added_dcc"] = sline[1]
            archive_metadata["archive_url"] = sline[2]

            #TODO - doesn't handle the classic archives!
            name_match = self.archive_patt.match(archive_metadata["archive_name"])
            if name_match is None:
                logging.warning("Skipping: archive %s does not name_match regex" % archive_metadata["archive_name"])
                continue

            archive_metadata["center_name"] = name_match.group(1)
            archive_metadata["disease_abbr"] = name_match.group(2)
            archive_metadata["platform"] = name_match.group(3)
            archive_metadata["data_type"] = name_match.group(4)
            archive_metadata["batch"] = name_match.group(5)
            archive_metadata["revision"] = int(name_match.group(6))
            #archive_metadata["series"] = name_match.group(7)

            
            logging.debug(archive_metadata["archive_url"])
            url_match = self.controlled_patt.match(archive_metadata["archive_url"])
            controlled = True

            if url_match is None:
                url_match = self.open_patt.match(archive_metadata["archive_url"])
                controlled = False

            logging.debug("controlled %s" % controlled)
            archive_metadata["controlled"] = controlled

            if url_match is None:
                logging.warning("Skipping: archive URL %s does not match regexes" % archive_metadata["archive_url"])
                continue

            logging.debug(url_match.groups())
            archive_metadata["center_type"] = url_match.group(2)
            archive_name_url = url_match.group(6)

            logging.debug(archive_name_url)

            #just a sanity check...
            if archive_name_url != archive_metadata["archive_name"] + ".tar.gz":
                logging.warning("Skipping: archive name in URL: %s does not match archive name in report: %s" 
                                % (archive_name_url,archive_metadata["archive_name"]))
                continue                                
            
            self._psql_sync_archive_metadata(archive_metadata)
        '''
        
