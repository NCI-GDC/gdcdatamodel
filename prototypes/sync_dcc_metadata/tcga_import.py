import os, sys
import json
import re
import logging
import settings
import requests
import psycopg2
from psycopg2 import extras
from datetime import datetime, tzinfo, timedelta

DEFAULTS = os.path.join(os.path.dirname(__file__),'defaults.json')
logging.basicConfig(level=logging.INFO)

#because python isoformat() isn't compliant without tz, alt is to use pytz
class SimpleUTC(tzinfo):
    def tzname(self):
        return "UTC"
    def utcoffset(self, dt):
        return timedelta(0)

class TCGAImporter:

    def __init__(self, path=DEFAULTS):
        #strict=False because the header string contains tabs
        self.dcc_settings = settings.Settings(path, "dcc", strict=False)
        self.couch_settings = settings.Settings(path, "couch")
        self.psql_settings = settings.Settings(path, "psql")

        #TODO put the regex in the config or something similar
        archive_regex = "(\S+)_(\w+)\.(\S+)\.(Level_3|Level_2|Level_1|mage-tab|aux)\.(\d+)\.(\d+)\.(\d+)"
        self.archive_patt = re.compile(archive_regex)

        self.open_patt = re.compile("%s(\w+)/(\w+)/(.+)/(.+)/(.+)/(.+)" % self.dcc_settings["open_base"])
        self.controlled_patt = re.compile("%s(\w+)/(\w+)/(.+)/(.+)/(.+)/(.+)"% self.dcc_settings["controlled_base"])
        
    def _get_dcc_latest(self):
        latest = requests.get(self.dcc_settings["latest"])

        if latest.status_code != 200:
            latest.raise_for_status()

        return latest.text

    def _add_metadata_fields(self, archive_metadata):
        archive_metadata["import_state"] = "not_started"
        archive_metadata["metadata_added"] = datetime.utcnow().replace(tzinfo=SimpleUTC()).replace(microsecond=0).isoformat()
        archive_metadata["import_host"] = None
        archive_metadata["md5sum_finish"] = None
        archive_metadata["import_start"] = None
        archive_metadata["import_finish"] = None
        archive_metadata["download_start"] = None
        archive_metadata["download_finish"] = None
        archive_metadata["md5sum_start"] = None
        archive_metadata["md5sum_finish"] = None
        archive_metadata["gdc_did"] = None

    def _psql_sync_archive_metadata(self, archive_metadata):
        self._add_metadata_fields(archive_metadata)

        logging.info("archive_metadata: %s"  % json.dumps(archive_metadata, indent=4, sort_keys=True))

        sets = settings.Settings()
        sets.pollServices()
        psql_host = sets.getServiceAddress('datastore')
        print psql_host

        conn = psycopg2.connect(
            host     = psql_host,
            dbname   = self.psql_settings["db"], 
            user     = self.psql_settings["user"], 
            password = self.psql_settings["passwd"]
        )
        cur = conn.cursor()
        #first check if exists
        #        cur.execute("SELECT * FROM tcga_dcc WHERE doc @> %s", (archive_metadata["center_name"],))
        cur.execute("SELECT doc::json FROM tcga_dcc WHERE doc @> %s", [extras.Json({"center_name" : archive_metadata["center_name"],
                                                                                   "disease_abbr" : archive_metadata["disease_abbr"],
                                                                                   "platform" : archive_metadata["platform"],
                                                                                   "data_type" : archive_metadata["data_type"],
                                                                                   "batch" : archive_metadata["batch"]})])

        add = True
        if cur.rowcount > 0:
            for record in cur:
                if record[0]["revision"] == archive_metadata["revision"]:
                    logging.info("Current revision already exists")
                    add = False
                elif record[0]["revision"] > archive_metadata["revision"]:
                    logging.error("Latest revision %d is less than stored revision %d" % 
                                  (archive_metadata["revision"], record[0]["revision"]))

        #just add for now - don't do anything with the old version...
        if add:
            logging.info("Adding archive_metadata %s" % json.dumps(archive_metadata, indent=4))
            cur.execute("INSERT INTO tcga_dcc (doc) VALUES (%s)", (json.dumps(archive_metadata, sort_keys=True),))

        conn.commit()
        cur.close()
        conn.close()

    #TODO right now below tied in with couch - need to abstract away...?
    #depends on by_archive_key view
    def _couch_sync_archive_metadata(self, archive_metadata):
        self._add_metadata_fields(archive_metadata)

        #stream of consciousness 
        archive_key = ".".join([archive_metadata["center_name"] + "_" + archive_metadata["disease_abbr"],
                                archive_metadata["platform"], archive_metadata["data_type"], archive_metadata["batch"]])
        logging.info(archive_key)
        check_url = "/".join([self.couch_settings["host"], self.couch_settings["name"], 
                              "_design", "find_docs", "_view", 'by_archive_key?key="%s"' % archive_key])

        r = requests.get(check_url,
                         auth=(self.couch_settings["user"], self.couch_settings["pwd"]))

        add_doc = False

        if r.status_code == 200:
            found_docs = r.json()
            if len(found_docs["rows"]) == 0:
                add_doc = True
            elif len(found_docs["rows"]) == 1:
                prev_doc = found_docs["rows"][0]["value"]
                if prev_doc["revision"] < archive_metadata["revision"]:
                    add_doc = True
                elif prev_doc["revision"] > archive_metadata["revision"]:
                    logging.warning("previous archive revision: %d greater than current revision: %d" % 
                                    (prev_doc["revision"], archive_metadata["revision"]))
            else:
                #this probably shouldn't be happening right now...
                logging.warning("more than one row found based on archive_key %s" % archive_key)
        else:
            logging.warning(r.json())
            #need some sort of exception here
            raise
                
        if add_doc:
            add_url = "/".join([self.couch_settings["host"], self.couch_settings["name"]])
            headers = {"content-type" : "application/json"}
            doc = json.dumps(archive_metadata, sort_keys=True)
            r = requests.post(add_url,
                              auth=(self.couch_settings["user"], self.couch_settings["pwd"]),
                              headers=headers,
                              data=doc)

            logging.info("after adding %s %s: %s" % (archive_metadata["archive_name"], r.status_code, r.text))
        else:
            logging.info("archive %s previously imported" % archive_metadata["archive_name"])
                          

    def import_latest_dcc(self):
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

        
