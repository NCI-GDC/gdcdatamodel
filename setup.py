from setuptools import setup, find_packages

setup(
    name="gdcdatamodel",
    use_scm_version={
        "local_scheme": "dirty-tag",
        "write_to": "gdcdatamodel/_version.py",
    },
    setup_requires=["setuptools_scm<6"],
    packages=find_packages(),
    install_requires=[
        "pytz~=2020.1",
        "graphviz>=0.4.10,<0.17",
        "jsonschema~=3.2",
        "futures; python_version=='2.7'",
        "functools32~=3.2; python_version=='2.7'",
        "gdcdictionary @ git+https://github.com/NCI-GDC/gdcdictionary.git@2.3.1#egg=gdcdictionary",
        "gdc-ng-models @ git+https://github.com/NCI-GDC/gdc-ng-models.git@1.5.2#egg=gdc-ng-models",
        "psqlgraph @ git+https://github.com/NCI-GDC/psqlgraph.git@3.3.0-rc.1#egg=psqlgraph",
    ],
    extras_require={
        "dev": [
            "pytest>=4.6.5,<6",
            "pytest-cov~=2.8.1",
            "pre-commit~=1.21.0",
            "cfgv~=2.0.1",
        ],
        "jupyter": [
            "notebook>4,<6.0.2",
            "jupyter-client>=4.2.2,<6",
            "jupyter-console>=4.1.1,<6",
            "jupyter-core>=4.1.0,<4.7",
        ]
    },
    package_data={
        "gdcdatamodel": [
            "xml_mappings/*.yaml",
        ]
    },
    entry_points={
        "console_scripts": ["gdc_postgres_admin=gdcdatamodel.gdc_postgres_admin:main"]
    },
)
