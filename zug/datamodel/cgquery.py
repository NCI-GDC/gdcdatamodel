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

def get_n_rows(start, rows, cghub_study):
    return query(
        cghub_study,
        start=start,
        rows=rows
    )

def get_changes_by_range(days_start, days_end, cghub_study):
    if days_end > days_start:
        log.warn('The ending day must be after the starting day, swapping the two')
        days_end, days_start = days_start, days_end
    return query(
        cghub_study,
        last_modified='[NOW-{}DAY TO NOW-{}]'.format(days_start, days_end)
    )

def get_changes_by_dt_range(start_dt, end_dt, cghub_study):
    if end_dt < start_dt:
        log.warn('The ending day must be after the starting day, swapping the two')
        start_dt, end_dt = end_dt, start_dt 
    return query(
        cghub_study,
        last_modified='[{} TO {}]'.format(start_dt.isoformat()+'Z', end_dt.isoformat()+'Z')
    )



def get_all(cghub_study):
    return query(cghub_study)

def get_changes_last_6_months(cghub_study):
    return query(cghub_study, last_modified='[NOW-6MONTH TO NOW]')
