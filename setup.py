from setuptools import setup, find_packages

setup(
    name='gdcdatamodel',
    version="2.0.0",
    packages=find_packages(),
    install_requires=[
        'pytz',
        'graphviz',
        'jsonschema',
        'gdcdictionary',
        'psqlgraph',
        'gdc-ng-models',
    ],
    package_data={
        "gdcdatamodel": [
            "xml_mappings/*.yaml",
        ]
    },
    entry_points={
        'console_scripts': [
            'gdc_postgres_admin=gdcdatamodel.gdc_postgres_admin:main'
        ]
    },
)
