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
        ]
    },
    dependency_links=[
        'git+ssh://git@github.com/NCI-GDC/cdisutils.git@4a75cc05c7ba2174e70cca9c9ea7e93947f7a868#egg=cdisutils',
        'git+ssh://git@github.com/NCI-GDC/psqlgraph.git@f0f198c2d7978fea311b0bc311c6db61732de261#egg=psqlgraph',
        'git+ssh://git@github.com/NCI-GDC/gdcdictionary.git@ae813717b3ce61983cea508d84fc056edb600a90#egg=gdcdictionary',
    ],
)
