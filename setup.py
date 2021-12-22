from setuptools import setup, find_packages
from subprocess import check_output


def get_version():
    # https://github.com/uc-cdis/dictionaryutils/pull/37#discussion_r257898408
    try:
        tag = check_output(
            ["git", "describe", "--tags", "--abbrev=0", "--match=[0-9]*"]
        )
        return tag.decode("utf-8").strip("\n")
    except Exception:
        raise RuntimeError(
            "The version number cannot be extracted from git tag in this source "
            "distribution; please either download the source from PyPI, or check out "
            "from GitHub and make sure that the git CLI is available."
        )


setup(
    name="gen3datamodel",
    version=get_version(),
    packages=find_packages(),
    install_requires=[
        "pytz>=2016.4",
        "graphviz~=0.4",
        "jsonschema~=3.2",
        "psqlgraph~=3.0",
        "cdisutils",
        "sqlalchemy~=1.3",
        "strict-rfc3339==0.7",
    ],
    package_data={
        "gdcdatamodel": [
            "xml_mappings/*.yaml",
        ]
    },
    entry_points={
        "console_scripts": ["gdc_postgres_admin=gdcdatamodel.gdc_postgres_admin:main"]
    },
)
