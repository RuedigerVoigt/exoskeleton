# Installation and Requirements

## The Database Backend

Exoskeleton uses MariaDB as a database backend. The library was developed using MariaDB 10.3.29. This or any newer version of MariaDB should be fine. You can check the version installed with the command `mysql --version`.

MariaDB claims to be a drop-in replacement for MySQL. They share a large part of their code-base. However, there are subtle differences like slightly different commands or different bugs. It was an explicit development aim to avoid those. So, using MySQL instead of MariaDB *should* work, but is untested.


1. Create a separate, empty database for your project.
1. Create a database user with all rights for this database. The crawler will use it to access and manage the queue. That account needs no permissions on other databases and therefore should not have them.
1. Now you need to create tables, views, stored procedures, triggers and more. Use the [Database-Script](https://github.com/RuedigerVoigt/exoskeleton/tree/master/Database-Scripts), but *do not forget to change the database name in the first two SQL commands*.



## Python and Python Packages

### The Right Version of Python

Exoskeleton needs at least Python 3.8. To check your python version, open a command line prompt and type one of these commands:
```bash
# If only a single version is installed:
python --version

# In case Python 2 and 3 are installed in parallel
# (for example Debian 10 or Ubuntu 19 systems):
python3 --version
```

Python 3.9 is tested and fully supported.
Beta versions of 3.10 are already used for testing.

### Installing with Pip

Install exoskeleton using `pip` or `pip3`. For example:
```bash
sudo pip install exoskeleton

# or if two versions of Python are installed:
sudo pip3 install exoskeleton
```

You may consider using [virtualenv](https://virtualenv.pypa.io/en/latest/ "Documentation") or [pipenv](https://pypi.org/project/pipenv/).

**The pip command should automatically install all dependencies which are not already part of the Python standard installation.**

These are:

* [aiodns](https://github.com/saghul/aiodns)
* [aiohttp](https://github.com/aio-libs/aiohttp)
* [beautifulsoup4](https://www.crummy.com/software/BeautifulSoup/ "beautiful soup project homepage"): A very useful package to analyze a webpage's code.
* [bote](https://github.com/RuedigerVoigt/bote): A sister package of exoskeleton which sends notifications.
* [compatibility](https://github.com/RuedigerVoigt/compatibility): A sister package of exoskeleton which ensures you run a suitable version of Python.
* [lxml](https://lxml.de/): A parser used to repair broken HTML code.
* [pymysql](https://github.com/PyMySQL/PyMySQL): Needed to connect to the MariaDB database.
* [requests](https://requests.readthedocs.io/en/master/): A high-level library to send and download data
* [urllib3](https://urllib3.readthedocs.io/en/latest/): The basis for the `requests` library
* [userprovided](https://github.com/RuedigerVoigt/userprovided): A sister package of exoskeleton which checks user input for plausibility.


### Updating Exoskeleton with Pip

```python
# pip or pip3 depending on your setup:
sudo pip install exoskeleton --upgrade
```



> :arrow_right: **[Now create a Bot](create-a-bot.md)**
