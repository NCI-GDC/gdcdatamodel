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
        'git+https://github.com/NCI-GDC/cdisutils.git@6686cdf8fa8fc9146f8eba5c95def713d66d8e07#egg=cdisutils',
        'git+https://github.com/NCI-GDC/psqlgraph.git@55897862b3a346f2194f6d24f59fa7c14c763602#egg=psqlgraph',
        'git+https://github.com/NCI-GDC/gdcdictionary.git@6a8ddf96ad59b44163c5091d80e04245db4a6e9a#egg=gdcdictionary',
    ],
    entry_points={
        'console_scripts': [
            'gdc_postgres_admin=gdcdatamodel.gdc_postgres_admin:main'
        ]
    },
)
