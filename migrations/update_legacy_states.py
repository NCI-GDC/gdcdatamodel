#!/usr/bin/env python

"""gdcdatamodel.migrations.update_legacy_states
----------------------------------

File nodes from legacy projects were given a `state` that represents
what is now `file_state`.  This script transforms the old `state` into
`file_state` and set's the `state` according the the following table:

| from file.state | to file.state | to file.file_state |
|-----------------+---------------+--------------------|
| None            | submitted     | None               |
| error           | validated     | error              |
| invalid         | validated     | error              |
| live            | submitted     | submitted          |
| submitted       | submitted     | registered         |
| uploaded        | submitted     | uploaded           |
| validated       | submitted     | validated          |

This script runs in parallel -> it has to use separate sessions -> it
has a session per Node subclass which is automatically committed.

See also https://jira.opensciencedatacloud.org/browse/DAT-276.

Usage:

```python
update_legacy_states(
    host='localhost',
    user='test',
    database='automated_test',
    password='test')
```

"""

import logging

from sqlalchemy import not_, or_, and_
from psqlgraph import Node, PsqlGraphDriver
from gdcdatamodel import models as md
from multiprocessing import Process, cpu_count, Queue
from collections import namedtuple


CLS_WITH_PROJECT_ID = {
    cls for cls in Node.get_subclasses() if "project_id" in cls.__pg_properties__
}


CLS_WITH_STATE = {
    cls for cls in Node.get_subclasses() if "state" in cls.__pg_properties__
}


CLS_TO_UPDATE = CLS_WITH_PROJECT_ID & CLS_WITH_STATE

# Determines state and file_state based on existing state
STATE_MAP = {
    None: {"state": "submitted", "file_state": None},
    "error": {"state": "validated", "file_state": "error"},
    "invalid": {"state": "validated", "file_state": "error"},
    "live": {"state": "submitted", "file_state": "submitted"},
    "submitted": {"state": "submitted", "file_state": "registered"},
    "uploaded": {"state": "submitted", "file_state": "uploaded"},
    "validated": {"state": "submitted", "file_state": "validated"},
}


logger = logging.getLogger("state_updater")
logging.basicConfig(level=logging.INFO)


def legacy_filter(query, legacy_projects):
    """filter query to those whose project_id is None or points to TARGET
    or TCGA

    """

    legacy_filters = [
        query.entity().project_id.astext
        == project.programs[0].name + "-" + project.code
        for project in legacy_projects
    ]

    return query.filter(or_(null_prop(query.entity(), "project_id"), *legacy_filters))


def null_prop(cls, key):
    """Provide expression to filter on a null or nonexistent value"""

    return or_(
        cls._props.contains({key: None}),
        not_(key in cls._props),
    )


def print_cls_query_summary(graph):
    """Print breakdown of class counts to stdout"""

    cls_queries = {
        cls.label: cls_query(graph, cls) for cls in CLS_WITH_PROJECT_ID & CLS_WITH_STATE
    }

    print(
        "%s: %d"
        % (
            "legacy_stateless_nodes".ljust(40),
            sum([query.count() for query in cls_queries.values()]),
        )
    )

    for label, query in cls_queries.items():
        count = query.count()
        if count:
            print("%35s : %d" % (label, count))


def cls_query(graph, cls):
    """Returns query for legacy nodes with state in {null, 'live'}"""

    legacy_projects = graph.nodes(md.Project).props(state="legacy").all()

    options = [
        # state
        null_prop(cls, "state"),
        cls.state.astext.in_(STATE_MAP),
    ]

    if "file_state" in cls.__pg_properties__:
        options += [null_prop(cls, "file_state")]

    return legacy_filter(graph.nodes(cls), legacy_projects).filter(or_(*options))


def update_cls(graph, cls):
    """Updates as described in update_target_states for a single class"""

    with graph.session_scope() as session:
        query = cls_query(graph, cls)
        count = query.count()
        if count == 0:
            return

        logger.info("Loading %d %s nodes", count, cls.label)
        nodes = query.all()
        logger.info("Loaded %d %s nodes", len(nodes), cls.label)

        for node in nodes:
            state = node._props.get("state", None)
            file_state = node._props.get("file_state", None)

            if state in STATE_MAP:
                node.state = STATE_MAP[state]["state"]

            set_file_state = (
                "file_state" in node.__pg_properties__
                and file_state is None
                and state in STATE_MAP
            )

            if set_file_state:
                node.file_state = STATE_MAP[state]["file_state"]

            node.sysan["legacy_state"] = state
            node.sysan["legacy_file_state"] = file_state

        logger.info("Committing %s nodes", cls.label)
        graph.current_session().commit()
        logger.info("Done with %s nodes", cls.label)


def update_classes(graph_kwargs, input_q):
    """Creates a db driver and pulls classes from the queue to update"""

    graph = PsqlGraphDriver(**graph_kwargs)

    while True:
        cls = input_q.get()
        if cls is None:  # none means no more work
            return

        update_cls(graph, cls)


def update_legacy_states(graph_kwargs):
    """Updates state, file_state on legacy nodes

    - node.state in {None, 'live'}
    - node.project_id in {None, <Legacy project_id list>}

    there is no project_id, or project_id points to a legacy project

    """

    graph = PsqlGraphDriver(**graph_kwargs)
    with graph.session_scope():
        print_cls_query_summary(graph)

    input_q = Queue()

    pool = [
        Process(target=update_classes, args=(graph_kwargs, input_q))
        for _ in range(cpu_count())
    ]

    for cls in CLS_TO_UPDATE:
        input_q.put(cls)

    for process in pool:
        input_q.put(None)  # put a no more work signal for each process

    for process in pool:
        process.start()

    for process in pool:
        process.join()
