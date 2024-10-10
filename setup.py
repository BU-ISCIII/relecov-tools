#!/usr/bin/env python

from setuptools import setup, find_packages

version = "1.2.0"

with open("README.md") as f:
    readme = f.read()

with open("requirements.txt") as f:
    required = f.read().splitlines()

setup(
    name="relecov_tools",
    version=version,
    description="Tools for managing and resolution of buisciii services.",
    long_description=readme,
    long_description_content_type="text/markdown",
    keywords=[
        "buisciii",
        "bioinformatics",
        "pipeline",
        "sequencing",
        "NGS",
        "next generation sequencing",
    ],
    author="Sara Monzon",
    author_email="smonzon@isciii.es",
    url="https://github.com/BU-ISCIII/relecov-tools",
    license="GNU GENERAL PUBLIC LICENSE v.3",
    entry_points={
        "console_scripts": ["relecov-tools=relecov_tools.__main__:run_relecov_tools"]
    },
    install_requires=required,
    packages=find_packages(exclude=("docs")),
    include_package_data=True,
    zip_safe=False,
)
