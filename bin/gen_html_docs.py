from gdcdatamodel import node_avsc_object, edge_avsc_object
from subprocess import check_call
from tempfile import NamedTemporaryFile
import json
import sys


def main():
    with NamedTemporaryFile(delete=False) as edges_json, NamedTemporaryFile(delete=False) as nodes_json:
        edges_json.write(json.dumps(edge_avsc_object.to_json()))
        nodes_json.write(json.dumps(node_avsc_object.to_json()))
        edges_json.flush()
        nodes_json.flush()
        check_call("avrodoc {} {} > docs/html/gdc_docs.html".format(nodes_json.name,
                                                                    edges_json.name),
                   shell=True,
                   stdout=sys.stdout,
                   stderr=sys.stderr)

if __name__ == "__main__":
    main()
