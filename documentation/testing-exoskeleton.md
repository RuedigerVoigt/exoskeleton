# Testing Exoskeleton

## Stability through Dependency Management

One project goal is to have a small number of dependencies outside of the Python standard library.

Integrating `beautifulsoup4`, `requests`, and `urllib3` was inevitable. Those however, are some of the most used libraries out there, actively developed and mature projects.

`pymysql` and `lxml` have shown me their worth in other projects which unfortunately are closed source. They could be replaced quite easily if ever necessary.

The libraries `userprovided` and `bote` are new and have a small user-base. Yet they are sister projects of `exoskeleton` and are jointly developed. The [userprovided library](https://github.com/RuedigerVoigt/userprovided "GitHub page for userprovided") contains most utility functions developed for exoskeleton and has 100% test coverage with unit tests.

The database backend is fixed to MariaDB. It would be nice to support multiple backends, but doing so would require another layer of software.

## Automated Testing for Pull Requests and Push

With each push to GitHub and each pull request, several automatic tests are triggered through GitHub Actions. Namely:
* A built on Ubuntu for all supported Python versions (as of July 2021: 3.8, 3.9 and 3.10 beta 3):
  * Installs all dependencies.
  * Run a syntax check using `flake8`.
  * Run unit tests with `pytest`.
  * A system test: The main functionalities of this framework are a mixture of Python and SQL code. This interacts with the MariaDB database, the network, and the operating system. Those also can introduce bugs (like #12). With every push or pull request a fresh database instance is spun up and a script loads exoskeleton and interacts with the database. During that the state of the database is automatically compared to what it should be.
*  For Windows and Mac: builts (without flake8)
* [mypy](http://mypy-lang.org/): a static type checker for Python that makes use of the type hints in the code. This for example catches missing or wrong return types, wrong parameters, and more.

## Bug Reports

*Bug reports are appreciated*. Please have a look into the [contributing guidelines](../contributing.md) before you submit them.

