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
    ],
    package_data={
        "gdcdatamodel": [
            "*.avsc",
            "avro/schemata/*.avsc",
        ]
    },
    dependency_links=[
        'git+ssh://git@github.com/NCI-GDC/psqlgraph.git@71294e1f94e2a6089d8440a1aa82cead9e59074f#egg=psqlgraph',
        'git+ssh://git@github.com/NCI-GDC/gdcdictionary.git@320a6c4b03d91238f54d681238d0bf29f8cef380#egg=gdcdictionary',
    ],
)
