from setuptools import setup, find_packages

setup(
    name="zug",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'pyyaml',
        'psqlgraph',
        'lxml',
        'requests',
        'psqlgraph',
        'gdcdatamodel',
    ],
    dependency_links=[
        'git+ssh://git@github.com/NCI-GDC/psqlgraph.git#egg=psqlgraph',
        'git+ssh://git@github.com/NCI-GDC/gdcdatamodel.git#egg=gdcdatamodel',
    ]
)
