from setuptools import setup, find_packages

setup(
    name='gdcdatamodel',
    packages=find_packages(),
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
