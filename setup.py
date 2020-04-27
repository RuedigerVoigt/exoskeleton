#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="exoskeleton",
    version="0.9.0",
    author="Rüdiger Voigt",
    author_email="projects@ruediger-voigt.eu",
    description="A library to create a bot / spider / crawler.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/RuedigerVoigt/exoskeleton",
    packages=setuptools.find_packages(),
    python_requires=">=3.6",
    install_requires=["beautifulsoup4", "lxml", "pymysql",
                      "requests", "urllib3", "userprovided>=0.5.3"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "Topic :: Text Processing :: Markup :: HTML",
        "Topic :: Internet :: WWW/HTTP :: Indexing/Search"
    ],
)
