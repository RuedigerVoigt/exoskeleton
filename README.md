# Exoskeleton

![Python package](https://github.com/RuedigerVoigt/exoskeleton/workflows/Python%20package/badge.svg)
![Supported Python Versions](https://img.shields.io/pypi/pyversions/exoskeleton)
![Last commit](https://img.shields.io/github/last-commit/RuedigerVoigt/exoskeleton)

For my dissertation I downloaded hundreds of thousands of documents and feed them into a machine learning pipeline. Using a high-speed-connection is helpful but carries the risk to run an involuntary denial-of-service attack on the servers that provide those documents. This creates a need for a crawler / scraper that avoids too high loads on the connection and instead runs permanently and fault tolerant to ultimately download all files.

Exoskeleton is a python framework that aims to help you build a similar bot. Main functionalities are:
* Managing a download queue within a MariaDB database.
* Avoid processing the same URL more than once.
* Working through that queue by either
    * downloading files to disk,
    * storing the page source code into a database table,
    * or making PDF-copies of webpages.
* Managing already downloaded files:
    * Storing multiple versions of a specific file.
    * Assigning labels to downloads, so they can be found and grouped easily.
* Sending progress reports to the admin.

**[Exoskeleton has an extensive documentation.](https://github.com/RuedigerVoigt/exoskeleton/tree/master/documentation "Learn about using exoskeleton")**
