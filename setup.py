from setuptools import find_packages, setup

setup(
    name="gdcdatamodel",
    use_scm_version={
        "local_scheme": "dirty-tag",
        "write_to": "gdcdatamodel/_version.py",
    },
    setup_requires=["setuptools_scm<6"],
    packages=find_packages(),
    install_requires=[
        "pytz~=2020.1",
        "graphviz>=0.4.10,<0.17",
        "jsonschema~=3.2",
        "pyrsistent<0.17.0",
        "decorator<=5.0.0",
        "gdcdictionary @ git+https://github.com/NCI-GDC/gdcdictionary.git@2.6.3#egg=gdcdictionary",
        "gdc-ng-models @ git+https://github.com/NCI-GDC/gdc-ng-models.git@1.5.2#egg=gdc-ng-models",
        "psqlgraph @ git+https://github.com/NCI-GDC/psqlgraph.git@feat/dev-1396-allow-multi-table-metadata#egg=psqlgraph",
    ],
    package_data={
        "gdcdatamodel": [
            "xml_mappings/*.yaml",
        ]
    },
    entry_points={
        "console_scripts": ["gdc_postgres_admin=gdcdatamodel.gdc_postgres_admin:main"]
    },
)
