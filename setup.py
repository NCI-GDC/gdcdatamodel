from setuptools import setup, find_packages

setup(
    name="zug",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'pyyaml',
        'psqlgraph',
        'gdcdatamodel',
        'cdisutils',
        'lxml==3.4.1',
        'requests==2.5.0',
        'apache-libcloud==0.15.1'
    ],
    dependency_links=[
        'git+ssh://git@github.com/NCI-GDC/psqlgraph.git@e2aabc16619bad0acbd4e754856990c27114a8e2#egg=psqlgraph',
        'git+ssh://git@github.com/NCI-GDC/cdisutils.git@6c3138bb946da6b68f860ed495f2889517a3b565#egg=cdisutils',
    ]
)
