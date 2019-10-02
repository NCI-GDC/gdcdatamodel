from setuptools import setup, find_packages

setup(
    name='gdcdatamodel',
    version="1.18.0",
    packages=find_packages(),
    install_requires=[
        'pytz',
        'graphviz',
        'jsonschema',
        'gdcdictionary',
        'psqlgraph',
        'gdc_ng_models',
    ],
    package_data={
        "gdcdatamodel": [
            "xml_mappings/*.yaml",
        ]
    },
    dependency_links=[
        'git+https://github.com/NCI-GDC/psqlgraph.git@release/py3#egg=psqlgraph',
        'git+https://github.com/NCI-GDC/gdcdictionary.git@release/py3#egg=gdcdictionary',
        'git+https://github.com/NCI-GDC/gdc-ng-models.git@release/redfish#egg=gdc_ng_models',
    ],
    entry_points={
        'console_scripts': [
            'gdc_postgres_admin=gdcdatamodel.gdc_postgres_admin:main'
        ]
    },
)
