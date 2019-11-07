# Exoskeleton

For my dissertation I download hundreds of thousands of documents and feed them into a ML system. Using a 1 Gbit/s connection is helpful, but carries the risk to run a involuntary denial-of-service attack on the servers that provide the documents.

That creates a need for a crawler / scraper that avoids too high loads on the connection, but runs permanently and fault tolerant to ultimately download all files.

Exoskeleton is a python framework that aims for that goal. It has three main functionalities:
* Managing a download queue within a SQL database.
* Working through that queue by downloading files to disk and page source code into a database table.
* Sending progress reports to the admin.

To analyze the content of a page I recommend the [Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/ "beautiful soup project homepage") package.

## Installation and Use

*Please take note that exoskeleton’s development status is "beta version".* This means it may still contain some bugs and some commands could change with one of the next releases.

1. Exoskeleton requires a database backend. Create a separate database for your project and create the necessary tables. You find scripts to create them on the GitHub project page within the folder named [Database-Scripts](https://github.com/RuedigerVoigt/exoskeleton/tree/master/Database-Scripts)
2. Create a database user with read / write / update rights for this database. The crawler will use it to access and manage the queue. That account needs no permissions on other database and therefore should not have them.
3. Install exoskeleton using `pip` or `pip3`. For example: `pip install exoskeleton`. You may consider using a [virtualenv](https://virtualenv.pypa.io/en/latest/userguide/ "Documentation").
4. Exoskeleton sets reasonable defaults, but you have to set at least some parameters. See the code examples below.
5. Add something to the queue and let exoskeleton do it's job.

### Examples

#### Basic Functionality

First create a database and a separate user for your bot. Then use the [Database-Script](https://github.com/RuedigerVoigt/exoskeleton/Database-Scripts) to create the table structure.

Put username and passphrase for the database into a separate file called `credentials.py`. If you store your bots in git, it might be a good idea to exclude the credentials file from uploads via the ignore list.

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File: credentials.py
user = 'databaseusername'
passphrase = 'secret_passphrase'
```

Now create a file that contains your bot:

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File: bot.py

import logging
import exoskeleton
import credentials

# exoskeleton makes heavy use of the built-in
# logging functionality. Change the level to
# INFO to see less messages.
logging.basicConfig(level=logging.DEBUG)

# create an object to setup the framework
queueManager = exoskeleton.Exoskeleton(
    database_host='ruediger-voigt.eu',
    database_name='exoskeleton',
    database_user=credentials.user,
    database_passphrase=credentials.passphrase
)

print(queueManager.num_items_in_queue())
print(queueManager.estimate_remaining_time())
```

Run the bot to see if the database connection works. The output with this setup should be:
```
INFO:root:You are using exoskeleton in version 0.5.0 (beta)
INFO:root:No port number supplied. Will try standard port instead.
WARNING:root:No mail address supplied. Unable to send emails.
WARNING:root:No mail address supplied. Unable to send emails.
WARNING:root:Target directory is not set. Using the current working directory /home/censored_path to store files!
DEBUG:root:Chosen hashing method is available on the system.
INFO:root:Hash method set to sha1
INFO:root:sha1 is fast, but a weak hashing algorithm. Consider using another method if security is important.
DEBUG:root:started timer
DEBUG:root:Trying to connect to database.
INFO:root:Made database connection.
DEBUG:root:Checking if the database table structure is complete.
DEBUG:root:Found table content
DEBUG:root:Found table eventLog
DEBUG:root:Found table files
DEBUG:root:Found table permanentErrors
DEBUG:root:Found table queue
DEBUG:root:Found table statisticsHosts
INFO:root:Found all expected tables.
0
WARNING:root:Cannot estimate remaining time as there are no data so far.
-1
```

There is nothing in the queue and it is not possible to estimate time as the crawler did not run. So let's change that by adding some things to the queue:

```python
queueManager.add_file_download('https://www.ruediger-voigt.eu/examplefile.txt')
queueManager.add_file_download('https://www.ruediger-voigt.eu/file_does_not_exist.pdf')
queueManager.add_save_page_code('https://www.ruediger-voigt.eu/')
```

Now tell your bot to work through the queue:

```python
queueManager.process_queue()
```

***After Exoskeleton worked through the queue, it will enter a wait state.***

The idea behind this behavior is, that multiple scripts can feed the queue. There might be the situation that the queue is empty, but new tasks will be entered some seconds later. So standard behavior for exoskeleton is to check the queue regulary.

You can change that behavior by setting an optional a parameter. Change the code above to:

```python
queueManager = exoskeleton.Exoskeleton(
    database_host='ruediger-voigt.eu',
    database_name='exoskeleton',
    database_user=credentials.user,
    database_passphrase=credentials.passphrase,
    queue_stop_on_empty=True # NEW
)
```

Now exoskelton will stop once the queue is empty.

#### Sending Progress Reports by Email

Exoskelton can send email when it reaches a milestone or finishes the job.


Note, that *it usually does not work to send email from a system with a dynamic ip-address* as most mail servers will classify them as spam. 
Even if you send from a machine with static IP many things might go wrong.
For example there might be a [SPF setting](https://en.wikipedia.org/wiki/Sender_Policy_Framework) for the sending domain.

For this reason the parameter `mail_send_start` defaults to True.
Once a sender and a receiver are defined, the bot tries to send an email.
Once you have a working setup, you can switch that off by setting the Parameter to `False`.
