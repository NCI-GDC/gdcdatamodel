from setuptools import setup, find_packages

setup(
    name='gdcdatamodel',
    use_scm_version={
        'local_scheme': 'dirty-tag',
        'write_to': 'gdcdatamodel/_version.py',
    },
    setup_requires=['setuptools_scm'],
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
