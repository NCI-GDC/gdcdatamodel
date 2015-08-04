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
            "datamodel/projects.csv",
            "datamodel/cghub_file_categorization.yaml",
            "datamodel/target/barcodes.tsv",
        ]
    },
    install_requires=[
        'progressbar==2.2',
        'networkx',
        'pyyaml',
        'psqlgraph',
        'gdcdatamodel',
        'cdisutils',
        'signpostclient',
        'ds3client',
        'lockfile',
        'lxml==3.4.1',
        'requests==2.5.2',
        'apache-libcloud==0.15.1',
        'cssselect==0.9.1',
        'elasticsearch==1.4.0',
        'pandas==0.15.2',
        'xlrd==0.9.3',
        'consulate==0.4',
        'boto==2.36.0',
        'filechunkio==1.6',
        'docker-py==1.2.2',
        # these next three are not used directly, but are required for
        # requests to have TLS SNI support, which we need for the
        # target dcc servers, plz don't remove them
        'pyOpenSSL==0.15.1',
        'ndg-httpsclient==0.4.0',
        'pyasn1==0.1.8',
    ],
    dependency_links=[
        'git+ssh://git@github.com/NCI-GDC/psqlgraph.git@621d7d157fad91bd0834280b8d2c39be384dcb1a#egg=psqlgraph',
        'git+ssh://git@github.com/NCI-GDC/cdisutils.git@99c87f23c6f955fed5971f987dbe36c92c9e4596#egg=cdisutils',
        'git+ssh://git@github.com/NCI-GDC/gdcdatamodel.git@264878ff7c3b591f8e272f40e489d2bda783d415#egg=gdcdatamodel',
        'git+ssh://git@github.com/NCI-GDC/python-signpostclient.git@63b3c1894299708d568506bce1ed3120fadbdf44#egg=signpostclient',
        'git+ssh://git@github.com/NCI-GDC/python-ds3-sdk.git@6a3382b3766d7e0898e9b70221d7e43f767acb8f#egg=ds3client',
    ]
)
