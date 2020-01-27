# Changelog / History

## version 0.7.1 beta (2020-01-27)

* If the user tries to add an URL which is already processed or in the queue, any new labels get associated with that item.

## version 0.7.0 beta (2020-01-16)

* Assign labels to pages in the queue / the filemaster
* Store MIME type / Media type
* Some settings cannot be set at init anymore, but are now stored in the database instead.
* Small but breaking changes to the database schema
* Change default file hash version from SHA1 to SHA256

## version 0.6.3 beta (2019-12-20)

* Eliminate race condition
* Prepare labels

## version 0.6.2 beta (2019-12-11)

* Add statistics on a per host basis.
* Improve error handling.


## version 0.6.1 beta (2019-12-07)

* Check if a file / URL has already been processed before adding it to the queue.

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
