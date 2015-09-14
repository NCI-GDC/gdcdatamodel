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
        'git+ssh://git@github.com/NCI-GDC/psqlgraph.git@fef50dd97ad37313b8a175134de66adae907b443#egg=psqlgraph',
        'git+ssh://git@github.com/NCI-GDC/gdcdictionary.git@320a6c4b03d91238f54d681238d0bf29f8cef380#egg=gdcdictionary',
    ],
)
