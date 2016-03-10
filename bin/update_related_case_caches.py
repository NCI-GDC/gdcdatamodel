# -*- coding: utf-8 -*-
"""update_related_case_caches
--------------------------

Will update the case tree for all non-legacy projects by caching the
related cases on all nodes.

"""

import argparse
import getpass
import logging

from gdcdatamodel import models as md
from gdcdatamodel.models.caching import CASE_CACHE_KEY
from psqlgraph import PsqlGraphDriver


logging.basicConfig()
logger = logging.getLogger("update_related_cases_caches")
logger.setLevel(logging.INFO)


def recursive_update_related_case_caches(node, case_id, visited_ids):
    """Sets the case_id in the cache on the source of all incoming edges
    and recurses.

    """

    logger.info("{}: | case: {} | project: {}".format(
        node, case_id, node._props.get('project_id', '?')))

    for edge in node.edges_in:
        original = set(edge.src.sysan.get(CASE_CACHE_KEY, []))
        updated = original.union({case_id})
        edge.src.sysan[CASE_CACHE_KEY] = list(updated)
        if edge.src_id not in visited_ids:
            recursive_update_related_case_caches(
                edge.src, case_id, visited_ids.union({edge.src_id}))


def update_project_related_case_cache(project):
    """Updates the case cache on the full tree under each case in the
    project

    """

    logger.info("Project: {}".format(project.code))
    for case in project.cases:
        recursive_update_related_case_caches(case, case.node_id, set())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-H", "--host", type=str, action="store",
                        required=True, help="psql-server host")
    parser.add_argument("-U", "--user", type=str, action="store",
                        required=True, help="psql test user")
    parser.add_argument("-D", "--database", type=str, action="store",
                        required=True, help="psql test database")
    parser.add_argument("-P", "--password", type=str, action="store",
                        help="psql test password")

    args = parser.parse_args()
    prompt = "Password for {}:".format(args.user)
    password = args.password or getpass.getpass(prompt)
    g = PsqlGraphDriver(args.host, args.user, password, args.database)

    with g.session_scope():
        projects = g.nodes(md.Project).not_props(state='legacy').all()
        map(update_project_related_case_cache, projects)

    print("Done.")

if __name__ == '__main__':
    main()
