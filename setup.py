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
        'git+ssh://git@github.com/NCI-GDC/psqlgraph.git@3394989c90e1cf4bfb017c2f51d1537a03cdcc2d#egg=psqlgraph',
        'git+ssh://git@github.com/NCI-GDC/gdcdictionary.git@b6325323c0e654840de5c4192e01cc89e431d5a6#egg=gdcdictionary',
    ],
)
