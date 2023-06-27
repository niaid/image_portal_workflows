#!/usr/bin/env python

from setuptools import setup, find_packages

with open("requirements.txt", "r") as fp:
    requirements = list(filter(bool, (line.strip() for line in fp)))

setup(
    name="em_workflows",
    version="1.2.0",
    author="Philip MacMenamin",
    author_email="bioinformatics@niaid.nih.gov",
    description="Workflows for Microscopy related file processing.",
    url="https://www.niaid.nih.gov/research/bioinformatics-computational-biosciences-branch",
    license="BSD 3-Clause.",
    classifiers=[
        "Development Status :: Release",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD 3-Clause.",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    packages=find_packages(include=["em_workflows", "em_workflows.*"]),
)
