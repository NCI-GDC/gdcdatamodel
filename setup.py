from setuptools import setup, find_packages

setup(
    name='gdcdatamodel',
    packages=find_packages(),
    install_requires=[
        'pytz==2016.4',
        'graphviz==0.4.2',
        'jsonschema==2.5.1',
        'python-dateutil==2.4.2',
        'psqlgraph',
        'gdcdictionary',
        'dictionaryutils',
        'cdisutils',
    ],
    package_data={
        "gdcdatamodel": [
            "xml_mappings/*.yaml",
        ]
    },
    dependency_links=[
        'git+https://github.com/NCI-GDC/cdisutils.git@4a75cc05c7ba2174e70cca9c9ea7e93947f7a868#egg=cdisutils',
        'git+https://github.com/NCI-GDC/psqlgraph.git@7b5de7d56aa3159a9526940eb273579ddbf084ca#egg=psqlgraph',
        'git+https://github.com/NCI-GDC/gdcdictionary.git@1.12#egg=gdcdictionary',
        'git+https://github.com/uc-cdis/dictionaryutils.git@2.0.0#egg=dictionaryutils',
    ],
    entry_points={
        'console_scripts': [
            'gdc_postgres_admin=gdcdatamodel.gdc_postgres_admin:main'
        ]
    },
)
