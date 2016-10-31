from setuptools import setup, find_packages

setup(
    name='gdcdatamodel',
    packages=find_packages(),
    install_requires=[
        'pytz==2016.4',
        'graphviz==0.4.2',
        'jsonschema==2.5.1',
        'psqlgraph',
        'gdcdictionary',
        'cdisutils',
        'python-dateutil==2.4.2',
    ],
    package_data={
        "gdcdatamodel": [
            "xml_mappings/*.yaml",
        ]
    },
    dependency_links=[
        'git+https://github.com/NCI-GDC/cdisutils.git@4a75cc05c7ba2174e70cca9c9ea7e93947f7a868#egg=cdisutils',
        'git+https://github.com/NCI-GDC/psqlgraph.git@4fb89ebee732e4ec4c4af02ad81e8dcd0f86f4cb#egg=psqlgraph',
        'git+https://github.com/NCI-GDC/gdcdictionary.git@fbf5f1279f55e18fe0d840ea2d9f0e58312aca8b#egg=gdcdictionary',
    ],
    entry_points={
        'console_scripts': [
            'gdc_postgres_admin=gdcdatamodel.gdc_postgres_admin:main'
        ]
    },
)
