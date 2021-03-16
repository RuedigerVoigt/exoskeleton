# Exoskeleton

![pypi version](https://img.shields.io/pypi/v/exoskeleton)
![Supported Python Versions](https://img.shields.io/pypi/pyversions/exoskeleton)
![Build](https://github.com/RuedigerVoigt/exoskeleton/workflows/Build/badge.svg)
![Last commit](https://img.shields.io/github/last-commit/RuedigerVoigt/exoskeleton)
[![Downloads](https://pepy.tech/badge/exoskeleton)](https://pepy.tech/project/exoskeleton)
[![Coverage](https://img.shields.io/badge/coverage-78%25-yellow)](https://www.ruediger-voigt.eu/coverage/exoskeleton/index.html)

For my dissertation I downloaded hundreds of thousands of documents and feed them into a machine learning pipeline. Using a high-speed-connection carries the risk to run an involuntary denial-of-service attack on the servers that provide those documents.

Exoskeleton is a Python framework that helps you build a crawler / scraper that avoids too high loads on the connection and instead runs permanently and fault tolerant to ultimately download all files.

Main functionalities are:
* Managing the download queue within a MariaDB database.
* Avoid processing the same URL more than once.
* Working through that queue by either
    * downloading files to disk,
    * storing the page source code into a database table,
    * storing the page text,
    * or making PDF-copies of webpages.
* Managing already downloaded files:
    * Storing multiple versions of a specific file.
    * Assigning labels to downloads, so they can be found and grouped easily.
* Sending progress reports to the admin.


# Documentation

## How To Use Exoskeleton

* [Installation and Requirements](https://github.com/RuedigerVoigt/exoskeleton/tree/master/documentation/installation.md)
* [Create a Bot](https://github.com/RuedigerVoigt/exoskeleton/tree/master/documentation/create-a-bot.md)
* [Dealing with result pages](https://github.com/RuedigerVoigt/exoskeleton/tree/master/documentation/parse-search-results.md)
* [Avoiding duplicates](https://github.com/RuedigerVoigt/exoskeleton/tree/master/documentation/avoiding-duplicates.md)
* [The Queue: Downloading files / Saving the page code / Creating PDF](https://github.com/RuedigerVoigt/exoskeleton/tree/master/documentation/handling-pages.md)
* [Bot Behavior](https://github.com/RuedigerVoigt/exoskeleton/tree/master/documentation/behavior-settings.md)
* [Progress Reports via Email](https://github.com/RuedigerVoigt/exoskeleton/tree/master/documentation/progress-reports-via-email.md)
* [File Versions and Labels](https://github.com/RuedigerVoigt/exoskeleton/tree/master/documentation/versions-and-labels.md)
* [Using the Blocklist](https://github.com/RuedigerVoigt/exoskeleton/tree/master/documentation/blocklist.md)

## Example Uses

* [Downloading an Archive](https://www.ruediger-voigt.eu/exoskeleton-download-an-archive.html) : A quite complex use case requiring some custom SQL. This is the actual project that triggered the development of exoskeleton.

## Technical Documentation

* [Contributing](https://github.com/RuedigerVoigt/exoskeleton/tree/master/contributing.md)
* [Database Structure](https://github.com/RuedigerVoigt/exoskeleton/tree/master/documentation/database-schema.md)
* [Testing](https://github.com/RuedigerVoigt/exoskeleton/tree/master/documentation/testing-exoskeleton.md)



## Example

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging

import exoskeleton

logging.basicConfig(level=logging.DEBUG)

# Create a bot
# exoskeleton makes reasonable assumptions about
# parameters left out, like:
# - host = localhost
# - port = 3306 (MariaDB standard)
# - ...
exo = exoskeleton.Exoskeleton(
    project_name='Bot',
    database_settings={'database': 'exoskeleton',
                       'username': 'exoskeleton',
                       'passphrase': ''},
    # True, to stop after the queue is empty, Otherwise it will
    # look consistently for new tasks in the queue:
    bot_behavior={'stop_if_queue_empty': True},
    filename_prefix='bot_',
    chrome_name='chromium-browser',
    target_directory='/home/myusername/myBot/'
)

exo.add_file_download('https://www.ruediger-voigt.eu/examplefile.txt')
# => Will be saved in the target directory. The filename will be the
#    chosen prefix followed by the database id and .txt.

exo.add_file_download(
    'https://www.ruediger-voigt.eu/examplefile.txt',
    {'example-label', 'foo'})
# => Duplicate will be recognized and not added to the queue,
#    but the labels will be associated with the file in the
#    database.


exo.add_file_download(
    'https://www.ruediger-voigt.eu/file_does_not_exist.pdf')
# => Nonexistent file: will be marked, but will not stop the bot.

# Save a page's code into the database:
exo.add_save_page_code('https://www.ruediger-voigt.eu/')

# Use chromium or Google chrome to generate a PDF of the website:
exo.add_page_to_pdf('https://github.com/RuedigerVoigt/exoskeleton')

# work through the queue:
exo.process_queue()
```
