from os import path

from setuptools import find_packages, setup

here = path.abspath(path.dirname(__file__))

with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="gdcdatamodel",
    description="Repo to keep information about the GDC data model design.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="NCI GDC",
    author_email="gdc_dev_questions-aaaaae2lhsbell56tlvh3upgoq@cdis.slack.com",
    url="https://github.com/NCI-GDC/gdcdatamodel",
    classifiers=[
        "Intended Audience :: Developers",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    license="Apache",
    packages=find_packages(),
    install_requires=[
        "pytz",
        "graphviz>=0.4.10",
        "jsonschema",
        "pyrsistent",
        "decorator",
        "gdcdictionary",
        "gdc-ng-models",
        "psqlgraph==6.0.4",
    ],
    extras_require={
        "dev": [
            "pytest",
            "pytest-cov",
        ],
        "jupyter": [
            "notebook",
            "jupyter",
            "jupyter-client",
            "jupyter-console",
            "jupyter-core",
        ],
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
