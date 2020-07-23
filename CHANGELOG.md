# Changelog / History

## upcoming 1.0.0

* Experimental Docker image

New Features:
* **System Test**: Each push and every pull requests now also triggers a system test. This test launches an Ubuntu instance and loads a MariaDB container. Then it creates the database, adds task to the queue and processes the queue.
* New function `add_save_page_text` which only saves the text of a page, but not its HTML code.
* The parameter `queue_max_retries` (in: `bot_behavior`) is now evaluated: After a task fails, the wait time until the next try increments. After the specified number of tries (default: 5) it is assumed an error is not temporary but permanent and exoskelton stop trying to execute the task.
* If a crawl delay is added to a specific task in the queue, it will now also be added to all other tasks that affect the same URL. The number of tries is still counted for each individual task, not the URL.
* If a crawler hits a rate limit, a server should respond with the HTTP status code 429 ("Too Many Requests"). If that is the case, exoskeleton now adds the fully qualified domain name (like `www.example.com`) to a rate limit list and blocks contact to this FQDN for a predefinied time (default: 31 minutes).
* Not all servers signal hitting the rate limit with the HTTP status code 429, but use codes like 404 ("Not Found") or 410 ("Gone") instead. Tasks that cause such errors stay in the queue, but exoskeleton does not try to carry them out. Therefore some helper functions were introduced to reset and start to process those again: `forget_all_errors`, `forget_permanent_errors`, `forget_temporary_errors`, and `forget_specific_error`.
* Added the database view `v_errors_in_queue` that makes information about errors occured while crawling better accessible.

Breaking Changes:
* The function `mark_error` has been renamed to `mark_permanent_error` to better reflect its purpose.
* The function `add_crawl_delay_to_item` was renamed to `__add_crawl_delay` and the optional parameter `delay_seconds` was removed.
* The database table `statistics_host`was extended.
* The method `get_queue_id` is now `QueueManager.__get_queue_uuids` as there is now reason to access it from a script.
* The method `num_items_in_queue` has been replaced with `Queuemanager.queue_stats` and now returns more information as a dictionary.

## version 0.9.3 beta (2020-07-18)

Breaking Changes:
* `__assign_labels_to_version` is now `assign_labels_to_uuid`
* `__assign_labels_to_master` is now `__assign_labels_to_master`
* Moved `utils.determine_file_extension()` into the `userprovided` project. Now it has to be called via `userprovided.url.determine_file_extension()`.
* Renamed database table `blocklist`to `blockList`: could be a breaking change depending on your system's settings.

New features:
* `version_uuids_by_label` gets the new option `processed_only`: if that is set to `True` it returns only UUIDs from files / content / ... that has been processed. Otherwise the returned set might include queue items with that label.
* New function `remove_labels_from_uuid` allows to remove one or more labels from an UUID / version.



## version 0.9.2 beta (2020-07-07)

Breaking Changes:
* Add a [blocklist](documentation/blocklist.md "How to use the blocklist"): requires changes to the database.
* Moved function `utils.get_file_hash()` into the userprovided library.
* Harmonized all functions that either return labels associated with ids or that return ids associated with labels to return the datatype `set`. If no associations are found the return value is an empty set.
* Issue #17 / Bugfix: The duplicate prevention did not work as expected for already processed files / pages. Had to extend the table `fileVersions` and the stored procedures.

New features:
* Adding to queue returns the UUID for the new queue item: The functions `add_page_to_pdf`, `add_save_page_code`, and `add_file_download` return the UUID *if* the task was added. In case the task was not added (for example because it is already in the queue or the host is in the blocklist) the return value is None. This functionality is needed for automatic tests, but it might also be useful to create some bots.
* Introduce new functions to organize files with labels:
    + `filemaster_labels_by_id`: Get a list of label names (not id numbers!) attached to a specific filemaster entry.
    + `get_filemaster_id`: Get the id of the filemaster entry associated with a specific version identified by its UUID.
    + `all_labels_by_uuid`: Get a set of ALL label names (not id numbers!) attached to a specific version of a file AND its filemaster entry.


## version 0.9.1 beta (2020-06-22)

Breaking Changes:
* There is a new parameter `mail_behavior` that contains the already existing settings `send_start_msg`, `send_finish_msg`, and `milestone_num`.
* The prefix `mail_` was removed in settings as all mail related settings are within the `mail_settings` dictionary. `mail_admin` was renamed to `recipient`.
* Move `utils.convert_to_set()` into the `userprovided` library.

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
* Add Ability to store any web page as PDF file using headless Chrome.
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
