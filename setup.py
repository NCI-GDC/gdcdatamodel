from setuptools import setup, find_packages

setup(
    name="zug",
    version="0.1",
    packages=find_packages(),
    package_data={
        "zug": [
            "datamodel/tcga_classification.yaml",
            "datamodel/centerCode.csv",
            "datamodel/tissueSourceSite.csv",
            "datamodel/bcr.yaml",
            "datamodel/cghub.yaml",
            "datamodel/clinical.yaml",
            "datamodel/cghub_file_categorization.yaml",
        ]
    },
    install_requires=[
        'pyyaml',
        'psqlgraph',
        'gdcdatamodel',
        'cdisutils',
        'signpostclient',
        'lockfile',
        'lxml==3.4.1',
        'requests==2.5.1',
        'apache-libcloud==0.15.1',
        'cssselect==0.9.1'
    ],
    dependency_links=[
        'git+ssh://git@github.com/NCI-GDC/psqlgraph.git@ab36169777266eec4c6eea02f52ba2ad46867fc7#egg=psqlgraph',
        'git+ssh://git@github.com/NCI-GDC/cdisutils.git@6c3138bb946da6b68f860ed495f2889517a3b565#egg=cdisutils',
        'git+ssh://git@github.com/NCI-GDC/gdcdatamodel.git@bd23909dd45a0ec97e6e9510dcc7d7840d101d58#egg=gdcdatamodel',
        'git+ssh://git@github.com/NCI-GDC/python-signpostclient.git@381e41d09dd7a0f9cd5f1ea5abea5bb1f34e9e70#egg=signpostclient',
    ]
)
