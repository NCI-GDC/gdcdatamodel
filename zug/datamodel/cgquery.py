import requests


url = 'https://cghub.ucsc.edu/cghub/metadata/analysisDetail'


def query_base(cghub_study, **q):
    q.update({'study': cghub_study})
    r = requests.get(url, params=q)
    r.encoding = 'UTF-8'
    return r.text


def get_changes_last_x_days(days, cghub_study):
    return query_base(
        cghub_study, last_modified='[NOW-{days}DAY TO NOW]'.format(days=days))


def get_all(cghub_study):
    query_base(cghub_study)


def get_changes_last_6_months(cghub_study):
    return query_base(cghub_study, last_modified='[NOW-6MONTH TO NOW]')
