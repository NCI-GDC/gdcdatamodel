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
        'numpy==1.10.4',
        'progressbar==2.2',
        'networkx',
        'pyyaml',
        'psqlgraph',
        'gdcdatamodel',
        'cdisutils',
        'signpostclient',
        'ds3client',
        'lockfile',
        'BeautifulSoup4==4.4.0',
        'lxml==3.4.1',
        'requests==2.6.0',
        'apache-libcloud==0.15.1',
        'cssselect==0.9.1',
        'elasticsearch==1.4.0',
        'pandas==0.15.2',
        'xlrd==0.9.3',
        'consulate==0.4',
        'boto==2.36.0',
        'filechunkio==1.6',
        'docker-py==1.2.2',
        'beautifulsoup4==4.4.0',
        # these next three are not used directly, but are required for
        # requests to have TLS SNI support, which we need for the
        # target dcc servers, plz don't remove them
        'pyOpenSSL==0.15.1',
        'ndg-httpsclient==0.4.0',
        'pyasn1==0.1.8',
        'datadog==0.9.0',
    ],
    dependency_links=[
        'git+ssh://git@github.com/NCI-GDC/psqlgraph.git@a00afd31a665420c5a5edd8b51fc2c7eb2889eb4#egg=psqlgraph',
        'git+ssh://git@github.com/NCI-GDC/cdisutils.git@99c87f23c6f955fed5971f987dbe36c92c9e4596#egg=cdisutils',
        'git+ssh://git@github.com/NCI-GDC/gdcdatamodel.git@35765ff5fe1cd771b4c0836cc909302e4d61c23f#egg=gdcdatamodel',
        'git+ssh://git@github.com/NCI-GDC/python-signpostclient.git@ebc91a5f8343e8f4cb2caefc523b9ad3a1eb7c6c#egg=signpostclient',
        'git+ssh://git@github.com/NCI-GDC/python-ds3-sdk.git@6a3382b3766d7e0898e9b70221d7e43f767acb8f#egg=ds3client',
    ]
)
