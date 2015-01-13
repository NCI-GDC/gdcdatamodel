from gdcdatamodel import node_avsc_object, edge_avsc_object
from subprocess import check_call
from tempfile import NamedTemporaryFile
import json
import sys
import os

# Load directory tree info
bin_dir = os.path.dirname(os.path.realpath(__file__))
doc_path = os.path.join(bin_dir, os.pardir, 'html', 'gdc_docs.html')


def main():
    with NamedTemporaryFile(delete=False) as edges_json, NamedTemporaryFile(
            delete=False) as nodes_json:
        edges_json.write(json.dumps(edge_avsc_object.to_json()))
        nodes_json.write(json.dumps(node_avsc_object.to_json()))
        edges_json.flush()
        nodes_json.flush()
        check_call(
            "avrodoc {} {} > {}".format(
                nodes_json.name, edges_json.name, doc_path),
            shell=True,
            stdout=sys.stdout,
            stderr=sys.stderr)

if __name__ == "__main__":
    print('Building html documentation')
    main()
