# Installation and Requirements

## The Database Backend

Exoskeleton uses MariaDB as a database backend. The library was developed using MariaDB 10.1.44. This or any newer version of MariaDB should be fine. Check the version installed with the command `mysql --version`.

MariaDB claims to be a drop-in replacement for MySQL. They share a large part of their codebase. However, there are subtle differences like slightly different commands or different bugs. It was an explicit development aim to avoid those. So, using MySQL instead of MariaDB should work, but is untested.


1. Create a separate, empty database for your project.
1. Create a database user with all rights for this database. The crawler will use it to access and manage the queue. That account needs no permissions on other databases and therefore should not have them.
1. Now you need to create tables, views, stored procedures, triggers and more. Use the [Database-Script](https://github.com/RuedigerVoigt/exoskeleton/tree/master/Database-Scripts), but *do not forget to change the database name in the first two SQL commands*.
1. Exoskeleton cursory checks the database setup once a bot is created.



## Python and Python Packages

### The Right Version of Python

Exoskeleton needs at least Python 3.6. To check your python version, open a command line prompt and type one of these commands:
```bash
# if only a single version is installed:
python --version

# if python 2 and 3 are installed in parallel
# (for example Debian 10 or Ubuntu 19 systems):
python3 --version
```

Newer versions than 3.6 like 3.7 or 3.8 are just equally fine and tested.

### Installing with Pip

Install exoskeleton using `pip` or `pip3`. For example:
```bash
pip install exoskeleton

# or if two versions of Python are installed:
pip3 install exoskeleton
```

You may consider using a [virtualenv](https://virtualenv.pypa.io/en/latest/userguide/ "Documentation").

**The pip command should automatically install all dependencies which are not already part of the Python standard installation.**

These are:

* [beautifulsoup4](https://www.crummy.com/software/BeautifulSoup/ "beautiful soup project homepage"): A very useful package to analyze a webpage's code.
* [lxml](https://lxml.de/): A parser used to repair broken HTML code
* [pymysql](https://github.com/PyMySQL/PyMySQL): Needed to connect to the MariaDB database.
* [requests](https://requests.readthedocs.io/en/master/): A high-level library to send and download data
* [urllib3](https://urllib3.readthedocs.io/en/latest/)
* [userprovided](https://github.com/RuedigerVoigt/userprovided): A sister package of exoskeleton which checks user input for plausibility.

### Updating Exoskeleton with Pip

```python
# pip or pip3 depending on your setup:
pip install exoskeleton --upgrade
```



> :arrow_right: **[Now create a Bot](create-a-bot.md)**