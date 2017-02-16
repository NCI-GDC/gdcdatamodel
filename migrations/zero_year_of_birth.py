#!/usr/bin/env python

"""gdcdatamodel.migrations.zero_year_of_birth
----------------------------------

We are not supposed to be exposing the year of birth for
people over 90 years old. As a result, anyone of that age who
is alive should have their age capped.

This script will check for all people alive at the time it is run
and change their birth year to < 90.

"""

import logging

from sqlalchemy import not_, or_, and_
from psqlgraph import Node, PsqlGraphDriver
from gdcdatamodel import models as m
import datetime

logger = logging.getLogger("zero_year_of_birth")
logging.basicConfig(level=logging.INFO)

def update_year_of_birth(graph_kwargs,
                         year_to_set=None,
                         dry_run=False):
    """Updates year_of_birth on nodes

    - node.state in {None, 'live'}
    - node.project_id in {None, <Legacy project_id list>}

    there is no project_id, or project_id points to a legacy project

    """
    year_boundary = str(datetime.datetime.now().year - 90)
    if not year_to_set:
        year_to_set = int(year_boundary ) + 1
    graph = PsqlGraphDriver(**graph_kwargs)
    with graph.session_scope() as session:
#        demo_nodes = g.nodes(m.Demographic)\
#                      .filter(Demographic._props['year_of_birth']
#                                         .astext <= year_boundary
#                      )\
#                      .path('cases.diagnoses')\
#                      .filter(Diagnosis._props['vital_status']
#                                       .astext == 'alive')\
#                      .all()
        demo_nodes = g.nodes(m.Demographic).all()
        logger.info('{} nodes found'.format(len(demo_nodes)))
        for node in demo_nodes:
            node.year_of_birth = year_to_set

        if dry_run:
            logger.info('Dry run, rolling back session')
            session.rollback()

if __name__ == '__main__':
    

