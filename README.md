[![Build Status](https://travis-ci.com/NCI-GDC/gdcdatamodel.svg?branch=develop)](https://travis-ci.org/NCI-GDC/gdcdatamodel)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commitlogoColor=white)](https://github.com/pre-commit/pre-commit)

---
GDC Data Model
==============

Repo to keep information about the GDC data model design.

- [GDC Data Model](#gdc-data-model)
- [Installation](#installation)
- [Jupyter + Graphviz](#jupyter--graphviz)
  - [Documentation](#documentation)
    - [Visual representation](#visual-representation)
  - [Dependencies](#dependencies)
    - [Project Dependencies](#project-dependencies)
- [Example validation usage](#example-validation-usage)
- [Tests](#tests)
- [Setup pre-commit hook to check for secrets](#setup-pre-commit-hook-to-check-for-secrets)
- [Contributing](#contributing)

# Installation

To install the gdcdatamodel library run the setup script:
```
❯ python setup.py install
```

# Jupyter + Graphviz

It's helpful to examine the relationships between nodes visually.  One
way to do this is to run a Jupyter notebook with a Python2 kernel.
When used with Graphviz's SVG support, you can view a graphical
representation of a subgraph directly in a REPL. To do so, install the
`dev-requirements.txt` dependencies.  There is an example Jupyter
notebook at `examples/jupyter_example.ipynb` (replicated in
`examples/jupyter_example.py` for clarity)

```
pip install -r dev-requirements
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

Project dependencies are managed using [PIP](https://pip.readthedocs.org/en/latest/)

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

# Tests

```
❯  nosetests -v
test_invalid_aliquot_node (test_avro_schemas.TestAvroSchemaValidation) ... ok
test_valid_aliquot_node (test_avro_schemas.TestAvroSchemaValidation) ... ok

----------------------------------------------------------------------
Ran 2 tests in 0.033s

OK
```



# Setup pre-commit hook to check for secrets

We use [pre-commit](https://pre-commit.com/) to setup pre-commit hooks for this repo.
We use [detect-secrets](https://github.com/Yelp/detect-secrets) to search for secrets being committed into the repo.

To install the pre-commit hook, run
```
pre-commit install
```

To update the .secrets.baseline file run
```
detect-secrets scan --update .secrets.baseline
```

`.secrets.baseline` contains all the string that were caught by detect-secrets but are not stored in plain text. Audit the baseline to view the secrets .

```
detect-secrets audit .secrets.baseline
```
# Contributing

Read how to contribute [here](https://github.com/NCI-GDC/portal-ui/blob/develop/CONTRIBUTING.md)
