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
        "six~=1.15",
        "pytz~=2020.1",
        "graphviz>=0.4.10,<0.17",
        "jsonschema~=3.2",
        "pyrsistent<0.17.0",
        "decorator<=5.0.0",
        "gdcdictionary @ git+https://github.com/NCI-GDC/gdcdictionary.git@2.5.0-rc.0#egg=gdcdictionary",
        "gdc-ng-models @ git+https://github.com/NCI-GDC/gdc-ng-models.git@1.5.2#egg=gdc-ng-models",
        "psqlgraph @ git+https://github.com/NCI-GDC/psqlgraph.git@3.4.0#egg=psqlgraph",
    ],
    extras_require={
        'python_version == "2.7"': [
            "futures~=3.3",
            "functools32~=3.2",
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
