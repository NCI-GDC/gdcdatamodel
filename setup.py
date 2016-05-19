from setuptools import setup, find_packages

setup(
    name='gdcdatamodel',
    packages=find_packages(),
    install_requires=[
        'avro==1.7.7',
        'graphviz==0.4.2',
        'addict==0.2.7',
        'jsonschema==2.5.1',
        'psqlgraph',
        'gdcdictionary',
        'cdisutils',
        'python-dateutil==2.4.2',
    ],
    package_data={
        "gdcdatamodel": [
            "*.avsc",
            "avro/schemata/*.avsc",
            "xml_mappings/*.yaml",
        ]
    },
    dependency_links=[
        'git+ssh://git@github.com/NCI-GDC/cdisutils.git@4a75cc05c7ba2174e70cca9c9ea7e93947f7a868#egg=cdisutils',
        'git+ssh://git@github.com/NCI-GDC/psqlgraph.git@a00ec5c0d3542e6f959e82fed0e46782479c6885#egg=psqlgraph',
        'git+ssh://git@github.com/NCI-GDC/gdcdictionary.git@5152302276dead9692240ddd735272949833c2ca#egg=gdcdictionary',
    ],
    entry_points={
        'console_scripts': [
            'gdc_postgres_admin=gdcdatamodel.gdc_postgres_admin:main'
        ]
    },
)
