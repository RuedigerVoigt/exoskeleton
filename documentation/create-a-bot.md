# Create an Exoskeleton Bot

In the previous step you [installed exoskeleton and prepared a database](installation.md). Now it is time to test the package by creating a bot.

## Store Credentials

You should avoid putting your credentials, like password and username, directly into your code. Many people accidentally upload code with secrets into public repositories. You might pass on your code to another project in order to get it started. This means you would have to remember every time to scrape your credentials.

### dotenv

The preferred way to store credentials would be to store them as environment variables. This can be tricky as per default those are not permanently stored and are not available after a reboot.

The [python-dotenv package](https://pypi.org/project/python-dotenv/) solves this problem by making the contents of a `.env` file available as environment variables. Accordingly `.env` should be added to your `.gitignore` file.

### Credentials File

Another OS independent and easy way is to put your username and passphrase for the database into a separate file called something like `credentials.py`.

Store this file outside your local repository or exclude it from uploads via the git ignore list. This file just defines some variables and could look like this:

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File: credentials.py
db = 'nameofyourdatabase'
user = 'databaseusername'
passphrase = 'secret_passphrase'
```


## Create a Bot

Now create a file `bot.py` that contains your bot:

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File: bot.py

import logging

import exoskeleton

# exoskeleton makes heavy use of the built-in
# logging functionality. Change the level to
# INFO to see less messages.
logging.basicConfig(level=logging.DEBUG)


exo = exoskeleton.Exoskeleton(
    database_settings={'database': 'database-name,
                       'username': 'database-user',
                       'passphrase': 'database-passphrase'},
    filename_prefix='TEST_',
    target_directory='/home/yourusername/testfolder/'
)
```

This is the bare minimum. If you run this code it should output something like this:

```
INFO:root:You are using exoskeleton 0.9.0 (beta / April 27, 2020)
WARNING:root:No hostname provided. Will try localhost.
INFO:root:No port number supplied. Will try standard port instead.
DEBUG:root:Trying to connect to database.
INFO:root:Established database connection.
DEBUG:root:Checking if the database table structure is complete.
DEBUG:root:Found table actions
DEBUG:root:Found table errorType
DEBUG:root:Found table fileContent
DEBUG:root:Found table fileMaster
DEBUG:root:Found table fileVersions
DEBUG:root:Found table jobs
DEBUG:root:Found table labels
DEBUG:root:Found table labelToMaster
DEBUG:root:Found table labelToVersion
DEBUG:root:Found table queue
DEBUG:root:Found table statisticsHosts
DEBUG:root:Found table storageTypes
INFO:root:Found all expected tables.
DEBUG:root:Checking if stored procedures exist.
DEBUG:root:Found stored procedure delete_all_versions_SP
DEBUG:root:Found stored procedure delete_from_queue_SP
DEBUG:root:Found stored procedure insert_content_SP
DEBUG:root:Found stored procedure insert_file_SP
DEBUG:root:Found stored procedure next_queue_object_SP
INFO:root:Found all expected stored procedures.
DEBUG:root:Set target directory to /home/exampleuser/TEST
DEBUG:root:Hash method sha256 is available.
DEBUG:root:started timers
```

You can (and probably should) later change the logging level to INFO, but here it gives you a good overview of what is checked.

If possible, parameters have plausible defaults. if you did not explicitly provide the information exoskeleton will try those defaults. An example for this:
```
WARNING:root:No hostname provided. Will try localhost.
INFO:root:No port number supplied. Will try standard port instead.
```

So exoskeleton assumed MariaDB runs on the same machine and the port is unchanged to standard 3306.

The dictionary `database_settings` knows the following parameters and defaults:

| Parameter  | Default Value |
| ------------- | ------------- |
| host  | localhost  |
| port  | 3306  |
| database  | :x:  |
| username  | :x:  |
| passphrase  | empty string  |

## First Test

Now let us *extend* the python code with:
```python
# Add two files to the queue for download
exo.add_file_download('https://www.ruediger-voigt.eu/examplefile.txt')
exo.add_file_download('https://github.com/RuedigerVoigt/exoskeleton/blob/master/README.md')

# Now tell exoskeleton to work through that queue:
exo.process_queue()
```

If you run this extended script exoskeleton downloads the first file, waits a bit, and downloads the next.

:heavy_exclamation_mark: *Notice*: In the default setup the bot will not stop after downloading these two files. Instead it will wait for 20 seconds and see if new jobs were added to the queue. You must stop the python program with (depending on your system) CTRL+C and/or CTRL+D.


Now look into the folder you specified with the `target_directory` parameter. The files are *not* named `examplefile.txt` and `README.md` but instead have the `filename_prefix` you specified followed by an alphanumeric id as name. The reason for such strange names is simple: if you download hundreds or thousands of files there will be many name collisions. That means you will have many files with the name `index.html` or similar which would overwrite each other. You would have to rename them into `index_a.html`, `index_b.html`, `index_c.html` and so on. *Here the alphanumeric id after the prefix is the id within the database. So, you can look up the source URL, the download date, and so on.*

> :arrow_right: **[Now learn how to parse search results](parse-search-results.md)**
