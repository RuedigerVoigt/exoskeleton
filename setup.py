#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import setuptools

from exoskeleton import _version

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="exoskeleton",
    version=f"{_version.__version__}",
    author="Rüdiger Voigt",
    author_email="projects@ruediger-voigt.eu",
    description="A library to create a bot / spider / crawler.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/RuedigerVoigt/exoskeleton",
    package_data={"exoskeleton": ["py.typed"]},
    packages=setuptools.find_packages(),
    python_requires=">=3.7",
    install_requires=[
        "aiodns>=3.0.0",
        "aiohttp>=3.7.4",
        "beautifulsoup4>=4.9.3",
        "bote>=1.2.0",
        "chardet>=4.0.0",
        "compatibility>=1.0.0",
        "lxml>=4.6.3",
        "pycares>=4.0.0",
        "pymysql>=1.0.2",
        "requests>=2.25.1",
        "urllib3>=1.26.6",
        "userprovided>=0.9.2"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "Topic :: Text Processing :: Markup :: HTML",
        "Topic :: Internet :: WWW/HTTP :: Indexing/Search"
    ],
)
