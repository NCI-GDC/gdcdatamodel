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
        'pytz~=2020.1',
        'future',
        'graphviz==0.4.10',
        'jsonschema',
        'gdcdictionary @ git+https://github.com/NCI-GDC/gdcdictionary.git@2.1.0#egg=gdcdictionary',
        'psqlgraph @ git+https://github.com/NCI-GDC/psqlgraph.git@3.0.0a1#egg=psqlgraph',
        'gdc-ng-models @ git+https://github.com/NCI-GDC/gdc-ng-models.git@1.2.0#egg=gdc-ng-models',
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
