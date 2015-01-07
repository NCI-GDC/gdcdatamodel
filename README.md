GDC Data Model
==============

Repo to keep information about the GDC data model design.

# Installation

To install the gdcdatamodel library run the setup script:
```
❯ python setup.py install
Setting up test database
Dropping old test data
Creating tables in test database
```

## Dependencies

Before continuing you must have the following programs installed:

- [Python 2.7+](http://python.org/)

The gdcdatamodel library requires the following pip dependencies

- [avro](https://avro.apache.org/)
- [graphviz](http://www.graphviz.org/)

### Project Dependencies

Project dependencies are managed using [PIP](https://pip.readthedocs.org/en/latest/)

# Tests

```
❯  nosetests -v
test_invalid_aliquot_node (test_avro_schemas.TestAvroSchemaValidation) ... ok
test_valid_aliquot_node (test_avro_schemas.TestAvroSchemaValidation) ... ok

----------------------------------------------------------------------
Ran 2 tests in 0.033s

OK
```
