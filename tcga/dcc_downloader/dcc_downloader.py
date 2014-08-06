import os, sys
import json
import logging
import settings
import requests
import hashlib
import subprocess
import time
import random
from datetime import datetime, tzinfo, timedelta

DEFAULTS = os.path.join(os.path.dirname(__file__),'defaults.json')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)-6s %(levelname)-4s %(message)s')

#these should probably go in some util module
def timestamp():
    return datetime.utcnow().replace(tzinfo=SimpleUTC()).replace(microsecond=0).isoformat()

def md5sum(filename):
    md5 = hashlib.md5()
    with open(filename, 'rb') as f:
        for chunk in iter(lambda: f.read(128 * md5.block_size), b''):
            md5.update(chunk)
    return md5.hexdigest()

def download_file(url, dl_dir="/tmp"):
    local_filename = os.path.join(dl_dir, url.split("/")[-1])
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
                f.flush()

    return local_filename

class ConflictException(Exception):
    pass


#because python isoformat() isn't compliant without tz, alt is to use pytz
class SimpleUTC(tzinfo):
    def tzname(self):
        return "UTC"
    def utcoffset(self, dt):
        return timedelta(0)

class DCCDownloader:

    def __init__(self, name, path=DEFAULTS):
        self.dcc_settings = settings.Settings(path, "tcga_dcc")
        self.did_settings = settings.Settings(path, "gdc_did")
        self.local_settings = settings.Settings(path, "local")
        
        self.name = name
        self.work = None
        self.doc_id = None
        self.search_url = "/".join([self.dcc_settings["host"], self.dcc_settings["table"], "_query"])

    '''
    This is for couchdb
    def update_state(self):
        update_url = "/".join([self.dcc_settings["host"], self.dcc_settings["index"], self.work["_id"]])
        r = requests.put(update_url,  auth=(self.dcc_settings["user"], self.dcc_settings["passwd"]),
                         data=json.dumps(self.work))

        if r.status_code == 201:
            logging.info(r.json())
            self.work["_rev"] = r.json()["rev"]
            logging.info("State successfully updated to %s " % json.dumps(self.work, indent=4,sort_keys=True))
        else:
            logging.info("Could not update state: %d %s" % (r.status_code, r.text))
            logging.info("Work at conflict: %s" % json.dumps(self.work, indent=4,sort_keys=True))
            #TODO appropriate error - this can happen if couchdb has updated, but elasticsearch hasn't
            #let's sleep a few seconds to see if the index catches up?
            time.sleep(3)
            raise ConflictException("throwing conflict exception")
    '''             

    #with postgres backend
    def update_state(self):
        update_url = "/".join([self.dcc_settings["host"], self.dcc_settings["table"], self.doc_id])
        r = requests.put(update_url,  auth=(self.dcc_settings["user"], self.dcc_settings["passwd"]),
                         data=json.dumps(self.work))

        if r.status_code != 201:
            r.raise_for_status()
   

    def resume_work(self, retry=True):
        if self.work is None:
            #first check if I have any work stored to resume 
            resume_query = '{ "import_host" : "%s", "import_finish":null}' % self.name

            #resume_query = '{  "query" : { "term" : { "import_host" : "%s" }}}' % self.name
            logging.info("search_url %s" % self.search_url)

            r = requests.get(self.search_url,  auth=(self.dcc_settings["user"], self.dcc_settings["passwd"]), data=resume_query)

            if r.status_code == 200:
                results = r.json()
                num_hits = results["num_results"]
                logging.info("resume num_hits %d" % num_hits)
                if num_hits > 0:
                    self.work = results["results"][0]["doc"]
                    self.doc_id = results["results"][0]["id"]
                    logging.info("resuming work: %s" % json.dumps(self.work, indent=4,sort_keys=True))
                else:
                    self._new_work()
                    logging.info("new work: %s" % json.dumps(self.work, indent=4,sort_keys=True))

            else:
                logging.error("error querying for resuming work: %d %s" % (r.status_code, r.text))
                return

        if self.work is None:
            logging.error("after trying to get work, still None")
            return

        workflow = [self._download, self._md5sum, self._upload]
        workflow_start = -1

        if self.work["import_state"] == "error" and retry:
            logging.info("Restart from beginning of error'd state")
            workflow_start = 0
            self.work["import_state"] = "host_assigned"
            #do we really want to delete this message?
            del self.work["message"]
            self.update_state()
        elif self.work["import_state"] == "host_assigned" or self.work["import_state"] =="downloading":
            workflow_start = 0
        elif self.work["import_state"] == "md5sum":
            workflow_start = 1
        elif self.work["import_state"] == "upload":
            workflow_start = 2
        else:
            logging.error("Unexpected import_state: %s" % self.work["import_state"])
            return

        logging.info("Starting from import_state %s %d" % (json.dumps(self.work["import_state"], indent=4,sort_keys=True), workflow_start))
        for f in workflow[workflow_start:]:
            logging.info("work before %s: %s" % (f.__name__, json.dumps(self.work, indent=4,sort_keys=True)))
            f()
            logging.info("work after %s: %s" % (f.__name__, json.dumps(self.work,indent=4,sort_keys=True)))
                         
        
            
    def _new_work(self):
        #new_query = '{  "query" : { "term" : { "import_state" : "not_started" }}}'
        #only do non-controlled data for now
        new_query = '{"import_state" : "not_started"}'
        
        r = requests.get(self.search_url, auth=(self.dcc_settings["user"], self.dcc_settings["passwd"]), data=new_query)

        if r.status_code == 200:
            results = r.json()
            num_hits = results["num_results"]
            logging.info("num_hits %d" % num_hits)
            if num_hits > 0:
                self.work = results["results"][0]["doc"]
                self.doc_id = results["results"][0]["id"]
                logging.info("assigning self %s" % json.dumps(self.work, indent=4,sort_keys=True))
                self.work["import_state"] = "host_assigned"
                self.work["import_host"] = self.name
                self.work["import_start"] = timestamp()
                self.update_state()
                #return self.claim_work()
            else:
                logging.info("no more work!")
        else:
            logging.error("error on request %s" % r.text)


    def _download(self, dl_dir="/tmp"):
        logging.info("starting download %s" % json.dumps(self.work["archive_name"], indent=4,sort_keys=True))
        self.work["import_state"] = "downloading"
        self.work["download_start"] = timestamp()
        self.update_state()

        local_md5sum = download_file(self.work["archive_url"] + ".md5", 
                                     dl_dir=self.local_settings["dl_dir"])
        local_file = download_file(self.work["archive_url"], 
                                   dl_dir=self.local_settings["dl_dir"])

        self.work["filesize"] = os.path.getsize(local_file)
        self.work["download_location"] = local_file
        self.work["download_finish"] = timestamp()
        self.update_state()

    #TODO - retry on failed md5sum
    def _md5sum(self):
        path = self.work["download_location"]
        md5_path = path + ".md5"
        self.work["import_state"] = "md5sum"
        self.work["md5sum_start"] = timestamp()
        self.update_state()
        dl_md5sum = md5sum(path)
        given_md5sum = open(md5_path).readlines()[0].split()[0].strip()

        self.work["md5sum_finish"] = timestamp()
        if dl_md5sum != given_md5sum:
            self.work["message"] = "calculated md5sum %s does not match given %s" % (dl_md5sum, given_md5sum)
            self.work["import_state"] = "md5sum_failed"
        else:
            self.work["md5"] = dl_md5sum
        
        self.update_state()

    def _update_did(self, did_info, did=None):
        logging.info("updating did %s %s" % (did, json.dumps(did_info, indent=4,sort_keys=True)))
        if did is None:
            update_url = self.did_settings["host"]
            r = requests.post(update_url, data=json.dumps(did_info))
        else:
            update_url = "/".join([self.did_settings["host"], did])
            curr_r = requests.get(update_url)

            if curr_r.status_code != 200:
                curr_r.raise_for_status()

            did_info["_rev"] = curr_r.json()["_rev"]
            r = requests.put(update_url, data=json.dumps(did_info))


        if r.status_code != 201:
            logging.error("Bad status code from GDC DID service %d %s" % (r.status_code, r.text))
            did = None
            self.work["import_state"] = "error"
            self.work["message"] = "GDC DID service failure %s %s" % (r.status_code, r.text)
            self.update_state()
            r.raise_for_status()

        logging.info("from did: %d %s" % (r.status_code, r.text))
        return r.json()["id"]


    def _upload(self):
        path = self.work["download_location"]
        self.work["import_state"] = "uploading"
        self.work["upload_start"] = timestamp()
        self.update_state()

        #first get GDC ID
        did_in = {}
        did_in["owner"] = "datamanager"
        did_in["url"] = None
        did_in["type"] = None

        if self.work["gdc_did"] is None:
            self.work["gdc_did"] = self._update_did(did_in)
            if self.work["gdc_did"] is None:
                #TODO real exception 
                raise

        object_name = os.path.basename(self.work["download_location"])
        segment_size = "1000000000"

        #TODO - this should be a call to swiftclient instead
        if self.work["filesize"] > segment_size:
            swift_cmd = ["swift", "upload", "--segment-size", segment_size, "--object-name", object_name, self.work["gdc_did"], self.work["download_location"]]
        else:
            swift_cmd = ["swift", "upload", "--object-name", object_name, self.work["gdc_did"], self.work["download_location"]]

        upload_proc = subprocess.Popen(swift_cmd)
        stdout, stderr = upload_proc.communicate()
        rc = upload_proc.returncode
        
        if rc != 0:
            self.work["import_state"] = "error"
            self.work["message"] = "swift upload failed: %s" % (stderr)
            self.update_state()
            return
            
        self.work["upload_finish"] = timestamp()
        #update DID

        did_in["url"] = "http://rados-bionimbus-pdc.opensciencedatacloud.org/v1/ALLISONHEATH/%s" % self.work["gdc_did"]
        did_in["type"] = "swift"
        did_update = self._update_did(did_in, did=self.work["gdc_did"])
    
        if did_update is None:
            logging.info("deleting uploaded container %s" % self.work["gdc_did"])
            swift_delete_cmd = ["swift", "delete", self.work["gdc_did"]]
            delete_proc = subprocess.Popen(swift_delete_cmd)
            stdout, stderr = delete_proc.communicate()
            rc = upload_proc.returncode
            if rc != 0:
                self.work["import_state"] = "error"
                self.work["message"] = "swift delete failed: %s" % (stderr)
                self.update_state()
            #TODO real exception
            logging.info("after deleting swift container: %s" % (json.dumps(self.work, indent=4,sort_keys=True)))
            raise

        #delete local file
        os.remove(self.work["download_location"])
        os.remove(self.work["download_location"] + ".md5")

        self.work["download_location"] = None
        self.work["import_state"] = "import_complete"
        self.work["import_finish"] = timestamp()
        self.update_state()


def main():
    n = 10000
    for i in range(0, n):
        logging.info("i:  %d" % i)
        try:
            #probably should reset instead of reinstantiate...?
            dl = DCCDownloader("test_worker")            
            dl.resume_work()
        except ConflictException:
            logging.info("passing on conflict exception %d" % i)
            sys.exit(-1)

if __name__ == "__main__":
    main()
    
