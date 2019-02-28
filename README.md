GDC Data Model
==============

Repo to keep information about the GDC data model design.

# Installation

To install the gdcdatamodel library run the setup script:
```
❯ python setup.py install
```

# Jupyter + Graphviz

It's helpful to examine the relationships between nodes visually.  One
way to do this is to run an Jupyter notebook with a Python2 kernal.
When used with Graphviz's SVG support, you can view a graphical
representation of a subgraph directly in a REPL. To do so, install the
`dev-requirements.txt` dependencies.  There is an example Jupyter
notebook at `examples/jupyter_example.ipynb` (replicated in
`examples/jupyter_example.py` for clarity)

```
pipenv install --dev
PG_USER=* PG_HOST=* PG_DATABASE=* PG_PASSWORD=*   jupyter notebook examples/jupyter_example.ipynb
```


## Documentation

### Visual representation

For instructions on how to build the Graphviz representation of the
datamodel, see the
[docs readme](https://github.com/NCI-GDC/gdcdatamodel/blob/develop/docs/README.md).


## Dependencies

Before continuing you must have the following programs installed:

- [Python 2.7+](http://python.org/)

The gdcdatamodel library requires the following pip dependencies

- [avro](https://avro.apache.org/)
- [graphviz](http://www.graphviz.org/)

### Project Dependencies

Project dependencies are managed using [Pipenv](https://pipenv.readthedocs.io/en/latest/)

# Example validation usage
```
from gdcdatamodel import node_avsc_object
from gdcdatamodel.mappings import get_participant_es_mapping, get_file_es_mapping
from avro.io import validate
import json


with open('examples/nodes/aliquot_valid.json', 'r') as f:
    node = json.loads(f.read())
print validate(node_avsc_object, node)  # if valid, prints True


print(get_participant_es_mapping())  # Prints participant elasticsearch mapping
print(get_file_es_mapping())         # Prints file elasticsearch mapping
```

# Example Elasticsearch mapping usage
```
from gdcdatamodel import mappings
print mappings.get_file_es_mapping()
print mappings.get_participant_es_mapping()
```

# Tests

```
❯  nosetests -v
test_invalid_aliquot_node (test_avro_schemas.TestAvroSchemaValidation) ... ok
test_valid_aliquot_node (test_avro_schemas.TestAvroSchemaValidation) ... ok

----------------------------------------------------------------------
Ran 2 tests in 0.033s

OK
```

# Contributing

Read how to contribute [here](https://github.com/NCI-GDC/portal-ui/blob/develop/CONTRIBUTING.md)
