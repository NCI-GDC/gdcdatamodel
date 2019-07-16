from setuptools import setup, find_packages

setup(
    name='gdcdatamodel',
    packages=find_packages(),
    install_requires=[
        'pytz==2016.4',
        'graphviz==0.4.2',
        'jsonschema==2.5.1',
        'python-dateutil==2.4.2',
        'pyasn1==0.4.2',
        'dictionaryutils',
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
        'git+https://github.com/uc-cdis/dictionaryutils.git@2.0.4#egg=dictionaryutils',
        'git+https://github.com/NCI-GDC/cdisutils.git@863ce13772116b51bcf5ce7e556f5df3cb9e6f63#egg=cdisutils',
        'git+https://github.com/NCI-GDC/psqlgraph.git@1.2.0#egg=psqlgraph',
        'git+https://github.com/NCI-GDC/gdcdictionary.git@qq-rc2#egg=gdcdictionary',
        'git+https://github.com/NCI-GDC/gdc-ng-models.git@1.0.0#egg=gdc_ng_models',
    ],
    entry_points={
        'console_scripts': [
            'gdc_postgres_admin=gdcdatamodel.gdc_postgres_admin:main'
        ]
    },
)
