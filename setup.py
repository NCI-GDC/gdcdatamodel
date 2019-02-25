from setuptools import setup, find_packages
from subprocess import check_output


def get_version():
    # https://github.com/uc-cdis/dictionaryutils/pull/37#discussion_r257898408
    try:
        tag = check_output(
            ["git", "describe", "--tags", "--abbrev=0", "--match=[0-9]*"]
        )
        return tag.strip("\n")
    except Exception:
        raise RuntimeError(
            "The version number cannot be extracted from git tag in this source "
            "distribution; please either download the source from PyPI, or check out "
            "from GitHub and make sure that the git CLI is available."
        )


setup(
    name='gdcdatamodel',
    version=get_version(),
    packages=find_packages(),
    install_requires=[
        'pytz>=2016.4',
        'graphviz>=0.4.2',
        'jsonschema>=2.5.1',
        'psqlgraph',
        'dictionaryutils>=1.2.0,<3.0.0',
        'cdisutils',
        'python-dateutil>=2.4.2',
    ],
    package_data={
        "gdcdatamodel": [
            "xml_mappings/*.yaml",
        ]
    },
    entry_points={
        'console_scripts': [
            'gdc_postgres_admin=gdcdatamodel.gdc_postgres_admin:main'
        ]
    },
)
