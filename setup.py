from setuptools import setup, find_packages

setup(
    name="zug",
    version="0.1",
    packages=find_packages(),
    package_data={
        "zug": [
            "classification.yaml",
        ]
    },
    install_requires=[
        'pyyaml',
        'psqlgraph',
        'gdcdatamodel',
        'cdisutils',
        'gdcdatamodel',
        'lxml==3.4.1',
        'requests==2.5.0',
        'apache-libcloud==0.15.1'
    ],
    dependency_links=[
        'git+ssh://git@github.com/NCI-GDC/psqlgraph.git@0b54ee8fc244a3306d91c80031db1ec131b9ddcb#egg=psqlgraph',
        'git+ssh://git@github.com/NCI-GDC/cdisutils.git@6c3138bb946da6b68f860ed495f2889517a3b565#egg=cdisutils',
        'git+ssh://git@github.com/NCI-GDC/gdcdatamodel.git@44d6a91c88135c04e3900827013bb7e98aab7e28#egg=gdcdatamodel',
    ]
)
