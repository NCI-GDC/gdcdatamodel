from setuptools import setup

setup(
    name='gdcdatamodel',
    packages=[
        'gdcdatamodel',
        'gdcdatamodel.mappings',
        'gdcdatamodel.models'
    ],
    install_requires=[
        'avro==1.7.7',
        'graphviz==0.4.2',
        'addict==0.2.7'
    ],
    package_data={
        "gdcdatamodel": [
            "*.avsc",
            "avro/schemata/*.avsc",
        ]
    },
)
