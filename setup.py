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
        'git+ssh://git@github.com/NCI-GDC/psqlgraph.git@97d44c2275107987d53f51a9b1488ad34e245bd1#egg=psqlgraph',
        'git+ssh://git@github.com/NCI-GDC/gdcdictionary.git@a662a4dfe7f002b03c84737c48426df6dc8b7c40#egg=gdcdictionary',
    ],
)
