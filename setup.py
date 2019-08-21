from setuptools import setup, find_packages

setup(
    name='gdcdatamodel',
    version="1.18.0",
    packages=find_packages(),
    install_requires=[
        'pytz==2016.4',
        'graphviz==0.4.2',
        'jsonschema==2.5.1',
        'python-dateutil==2.4.2',
        'pyasn1==0.4.2',
        'gdcdictionary',
        'psqlgraph',
        'cdisutils',
        'gdc_ng_models',
    ],
    package_data={
        "gdcdatamodel": [
            "xml_mappings/*.yaml",
        ]
    },
    dependency_links=[
        'git+https://github.com/NCI-GDC/cdisutils.git@863ce13772116b51bcf5ce7e556f5df3cb9e6f63#egg=cdisutils',
        'git+https://github.com/NCI-GDC/psqlgraph.git@chore/2to3-099to13#egg=psqlgraph',
        'git+https://github.com/NCI-GDC/gdcdictionary.git@feat/TT-1136-port-dictionaryutils#egg=gdcdictionary',
        'git+https://github.com/NCI-GDC/gdc-ng-models.git@1.0.1#egg=gdc_ng_models',
    ],
    entry_points={
        'console_scripts': [
            'gdc_postgres_admin=gdcdatamodel.gdc_postgres_admin:main'
        ]
    },
)
