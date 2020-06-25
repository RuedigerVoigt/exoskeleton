# Exoskeleton Tests

Most utility functions developed for exoskeleton are now part of the [userprovided library](https://github.com/RuedigerVoigt/userprovided "GitHub page for userprovided"), which has 100% test coverage.

Some functions in exoskeleton are checked with unit tests. The main functionalities however are a mixture of Python and SQL code. This interacts with the MariaDB database and the operating system. Those also can introduce bugs (like #12).

Therefore in testing with every push to GitHub a fresh database instance is spun up and the [validation.py](validation.py) script loads exoskelton and interacts with the database. During that the state of the database is compared to what it should be.

