from setuptools import setup

setup(
    name='gdcdatamodel',
    packages=["gdcdatamodel"],
    install_requires=[
        'avro==1.7.7',
        'graphviz',
    ],
)
