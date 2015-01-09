import requests
import logging
import re
from datetime import tzinfo, timedelta

logger = logging.getLogger(name="[{name}]".format(name=__name__))

default_latest_firstline = 'ARCHIVE_NAME DATE_ADDED ARCHIVE_URL'.split()
default_latest_url = "http://tcga-data.nci.nih.gov/datareports/resources/latestarchive"
default_open_url = "https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/"
default_protected_url = "https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/tcga4yeo/tumor/"


# because python isoformat() isn't actually compliant without tz
class SimpleUTC(tzinfo):
    def tzname(self):
        return "UTC"

    def utcoffset(self, dt):
        return timedelta(0)


class LatestURLParser(object):

    def __init__(self, open_url=default_open_url,
                 latest_firstline=default_latest_firstline,
                 latest_url=default_latest_url,
                 protected_url=default_protected_url, constraints={},
                 url_key=None,
                 **kwargs):

        self.open_url = open_url
        self.latest_firstline = latest_firstline
        self.latest_url = latest_url
        self.protected_url = protected_url
        self.constraints = constraints
        self.url_key = url_key

        # get the latest archive files
        latest_report = self.pull_dcc_latest().splitlines()
        header = latest_report.pop(0).split()

        # verify the file has the expected header
        if header != self.latest_firstline:
            print self.latest_firstline
            logger.error('Unexpected header for latest report: {}'.format(
                header))
            raise Exception('Unexpected header for latest report: {}'.format(
                header))

        self.latest_report = latest_report

    def get_archives(self):
        return self.__iter__()

    def __iter__(self):
        while len(self.latest_report):
            archive = self.next()
            if archive:
                yield archive

    def next(self):
        line = self.latest_report.pop(0)
        sline = line.strip().split('\t')
        archive = self.parse_archive(*sline)
        for key, value in self.constraints.iteritems():
            if archive[key] != value:
                return None

        if self.url_key:
            ret = archive[self.url_key]
            logging.info('Found url: {}'.format(ret))
        else:
            ret = archive

        return ret

    def pull_dcc_latest(self):
        r = requests.get(self.latest_url)
        if r.status_code != 200:
            return None
        return r.text

    def parse_archive_name(self, archive):
        archive_name = archive['archive_name']

        pattern = re.compile(r"""
        ^
        (.+?)_   # center
        (\w+)\.  # disease code
        (.+?)\.  # platform
        (.+?\.)? # data level, ABI archives do not have data level part
        (\d+?)\. # batch
        (\d+?)\. # revision
        (\d+)    # series
        $
        """, re.X)

        m = re.search(pattern, archive_name)
        archive['center_name'] = m.group(1)
        archive['disease_code'] = m.group(2)
        archive['platform'] = m.group(3)
        archive['batch'] = int(m.group(5))
        archive['revision'] = int(m.group(6))

        if m.group(4):
            archive['data_level'] = m.group(4).replace('.', '')
        else:
            archive['data_level'] = None

    def parse_archive_url(self, archive):
        archive_url = archive['dcc_archive_url']
        open_base_url = self.open_url
        protected_base_url = self.protected_url

        if (archive_url.startswith(open_base_url)):
            # open archive
            archive['protected'] = False

        elif (archive_url.startswith(protected_base_url)):
            # protected archive
            archive['protected'] = True

        else:
            logger.warning('Unmatched archive URL: ' + archive_url)

        logger.debug('archive_url: %s' % archive_url)
        parts = archive_url.split('/')

        if (parts[8].upper() != archive['disease_code']):
            logger.warning("Unmatched disease code between Archive URL and "
                           "Archive Name: " + parts[8] + " vs " +
                           archive['disease_code'])
        if (parts[10] != archive['center_name']):
            logger.warning("Unmatched center_name between Archive URL and "
                           "Archive Name: " + parts[10] + " vs " +
                           archive['center_name'])

        archive['center_type'] = parts[9]
        archive['platform_in_url'] = parts[11]
        archive['data_type_in_url'] = parts[12]

    def parse_archive(self, archive_name, date_added, archive_url):
        archive = {}
        archive['_type'] = 'tcga_dcc_archive'
        archive['archive_name'] = archive_name
        archive['date_added'] = date_added
        archive['dcc_archive_url'] = archive_url

        self.parse_archive_name(archive)
        self.parse_archive_url(archive)
        return archive

if __name__ == '__main__':
    lurls = LatestURLParser()
    for doc in lurls:
        print doc
