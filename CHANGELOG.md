# Changelog / History

## version 0.9.1 beta (2020-06-22)

Breaking Changes:
* There is a new parameter `mail_behavior` that contains the already existing settings `send_start_msg`, `send_finish_msg`, and `milestone_num`.
* The prefix `mail_` was removed in settings as all mail related settings are within the `mail_settings` dictionary. `mail_admin` was renamed to `recipient`.

New Features / Improvements:
* New function `version_uuids_by_label`: returns all UUIDs which have this label attached.
* New function `version_labels_by_uuid`: return all labels attached to a specific version / UUID, but not the filemaster entry.
* The functionality to send mails has been externalized into the [`bote` library](https://github.com/RuedigerVoigt/bote "Homepage of the bote project"). As it has the same maintainer, compatibility is ensured.
* It is now possible to send mail with another server than localhost.
* Signal compliance with [PEP 561](https://www.python.org/dev/peps/pep-0561/): If you type-check code that imports this package, tools like mypy now know that `userprovided` has type-hints and extend their checks to calls of these functions.
* The automatic tests now also cover Windows and MacOS.

## version 0.9.0 beta (2020-04-27)

Breaking Changes:
* Changed the database structure especially regarding labels. (Reason: issue #12)
* Several class parameters were joined into dictionaries.

New Features / Improvements:
* Added an extensive [documentation](documentation/README.md).
* Restore lost database connection after timeout.
* Add Ability to store any webpage as PDF file using headless Chrome.
* Prettify HTML before storing it.
* The content header is used to determine the file extension.
* Build process improved: with GitHub-Actions syntax checks and unit-tests are run for each build in Python 3.6, 3.7 and 3.8.

Other:
* Input checks were moved to the sister package [userprovided](https://github.com/RuedigerVoigt/userprovided)


## version 0.8.2 beta (2020-02-21)

* Bugfix in job_get_current_url()

## version 0.8.1 beta (2020-02-18)

* Add ability to create jobs: In essence a convenient way to store the current URL while parsing a large set of results. In case the program is terminated, now it is easy to pick up at the same page.

## version 0.8.0 beta (2020-02-14)

* Require Python 3.6 (Debian 10 "Buster" just moved to Python 3.7 with the 10.3 update. Ubuntu stable uses 3.6.)
* Use transactions and rollbacks to ensure dbms integrity (i.e. changes to the database schema).

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
