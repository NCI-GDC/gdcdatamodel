# Installation

## Dependencies

Before continuing you must have the following programs installed:

- [Python 2.7+](http://python.org/)
- [Postgresql 9.4](http://www.postgresql.org/download/)

The zug library requires the following pip dependencies

- [psqlgraph](https://github.com/NCI-GDC/psqlgraph)

### Project Dependencies

Project dependencies are managed using [PIP](https://pip.readthedocs.org/en/latest/)

## Test Setup

Running the setup script will:

1. Setup the test postgres tables for datamodel store

```
❯ python bin/setup_psql_graph.py
Setting up test database
Dropping old test data
Creating tables in test database
```

# Tests

Running the setup script will test the library against a local postgres installation

```
❯  cd test; nosetests -v
