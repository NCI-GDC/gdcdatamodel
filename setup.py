from setuptools import find_packages, setup

setup(
    name="gdcdatamodel",
    use_scm_version={
        "local_scheme": "dirty-tag",
        "write_to": "gdcdatamodel/_version.py",
    },
    setup_requires=["setuptools_scm<6"],
    packages=find_packages(),
    install_requires=[
        "pytz",
        "graphviz>=0.4.10",
        "jsonschema",
        "pyrsistent",
        "decorator",
        "gdcdictionary @ git+https://github.com/NCI-GDC/gdcdictionary.git@2.6.6#egg=gdcdictionary",
        "gdc-ng-models @ git+https://github.com/NCI-GDC/gdc-ng-models.git@1.6.4#egg=gdc-ng-models",
        "psqlgraph @ git+https://github.com/NCI-GDC/psqlgraph.git@4.0.1#egg=psqlgraph",
    ],
    extras_require={
        "dev": [
            "dataclasses; python_version < '3.7'",
            "pytest",
            "pytest-cov",
        ],
        "jupyter": [
            "notebook",
            "jupyter",
            "jupyter-client",
            "jupyter-console",
            "jupyter-core"
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
