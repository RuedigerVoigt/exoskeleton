# Changelog / History

## version 0.6.0 beta (2019-11-26)

* *Breaking Change*: Changed database structure in order to manage multiple versions of a file in different locations (local / Cloud / ...).
* Using database triggers to increment / decrement the number of versions saved.
* Add file name prefix
* Allow database access without passphrase
* Improved error handling
* Extended documentation (especially for the [Database Structure](Database-Scripts/README.md))
* Introduced unit tests

## version 0.5.2 beta (2019-11-07)

* Removed f-strings to be compatible with Python 3.5 as used by Debian 9.
* Add URL specific delays (for example in case of timeouts / #7)
* minor bug fixes

## version 0.5.0 beta (2019-10-25)


* Initial public release
