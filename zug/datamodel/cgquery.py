import requests
from cdisutils.log import get_logger

log = get_logger("cgquery")

url = 'https://cghub.ucsc.edu/cghub/metadata/analysisFull'

def query(cghub_study, **q):
    q.update({'study': cghub_study})
    log.info('Query for {}'.format(q))
    r = requests.get(url, params=q)
    return r.content

def get_changes_last_x_days(days, cghub_study):
    return query(
        cghub_study,
        last_modified='[NOW-{days}DAY TO NOW]'.format(days=days)
    )

def get_all(cghub_study):
    return query(cghub_study)

def get_changes_last_6_months(cghub_study):
    return query(cghub_study, last_modified='[NOW-6MONTH TO NOW]')
