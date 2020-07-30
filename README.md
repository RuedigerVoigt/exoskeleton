# Exoskeleton

![Build](https://github.com/RuedigerVoigt/exoskeleton/workflows/Build/badge.svg)
![System Test](https://github.com/RuedigerVoigt/exoskeleton/workflows/System%20Test/badge.svg)
![Supported Python Versions](https://img.shields.io/pypi/pyversions/exoskeleton)
![Last commit](https://img.shields.io/github/last-commit/RuedigerVoigt/exoskeleton)
![pypi version](https://img.shields.io/pypi/v/exoskeleton)
[![Downloads](https://pepy.tech/badge/exoskeleton)](https://pepy.tech/project/exoskeleton)

For my dissertation I downloaded hundreds of thousands of documents and feed them into a machine learning pipeline. Using a high-speed-connection is helpful but carries the risk to run an involuntary denial-of-service attack on the servers that provide those documents. This creates a need for a crawler / scraper that avoids too high loads on the connection and instead runs permanently and fault tolerant to ultimately download all files.

Exoskeleton is a python framework that aims to help you build a similar bot. Main functionalities are:
* Managing a download queue within a MariaDB database.
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

**[Exoskeleton has an extensive documentation.](https://github.com/RuedigerVoigt/exoskeleton/tree/master/documentation "Learn about using exoskeleton")**


Two other python libraries were created as part of this project:
* [userprovided](https://github.com/RuedigerVoigt/userprovided/ "Code and documentation for userprovided") : check user input for validity and plausibility / covert input into better formats
* [bote](https://github.com/RuedigerVoigt/bote/ "Code and documentation for bote") : send messages (currently via a local or remote SMTP server)

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
