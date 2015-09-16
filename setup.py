from setuptools import setup, find_packages

setup(
    name='gdcdatamodel',
    packages=find_packages(),
    install_requires=[
        'avro==1.7.7',
        'graphviz==0.4.2',
        'addict==0.2.7',
        'psqlgraph',
        'gdcdictionary',
        'python-dateutil==2.4.2',
    ],
    package_data={
        "gdcdatamodel": [
            "*.avsc",
            "avro/schemata/*.avsc",
        ]
    },
    dependency_links=[
        'git+ssh://git@github.com/NCI-GDC/psqlgraph.git@b4bb4ea9ae8631b245d61621093eb5702d33fbb7#egg=psqlgraph',
        'git+ssh://git@github.com/NCI-GDC/gdcdictionary.git@10f8a5690a42ee659b47e56371bc4615ef845ffa#egg=gdcdictionary',
    ],
)
