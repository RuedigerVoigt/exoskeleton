#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Automatic Tests for exoskeleton

! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! !  ! ! ! ! ! ! ! ! !
There are two groups of automatic tests for exoskeleton:
* Unit-tests without side-effects that run extremly fast and cover about 1/3.
* A system-test that actually interacts with a database and the network.

This file contains the latter group of tests.
! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! !  ! ! ! ! ! ! ! ! !

While the system-test is much slower, it also tests the SQL code and the code
interacting with the network. It simulates a user interacting with the
framework.

Even though much *could* be mocked, it is better to actually run it:
There might be changes to the SQL part and sometimes the DBMS itself
might introduce a bug.

This kind of test needs a service container with a running MariaDB instance.
At this moment (Feb 2021) those are only supported for Linux:
https://docs.github.com/en/actions/guides/about-service-containers

To run only the tests here:
coverage run --source exoskeleton -m pytest tests_with_side_effects.py
To generate a report limited to that run afterwards:
coverage html
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2021 Rüdiger Voigt
Released under the Apache License 2.0
"""

from collections import Counter
import hashlib
import logging
import subprocess
from unittest.mock import patch

import pymysql
import pytest

import exoskeleton
from exoskeleton import exo_url
from exoskeleton import err

logging.basicConfig(level=logging.DEBUG)

# #############################################################################
# CREATE INSTANCES OF EXOSKELETON
# #############################################################################

DB_PORT = 12345
BROWSER = 'chromium-browser'

logging.info('Create an instance to use in furter tests')

# Create an instance that is used for tests
exo = exoskeleton.Exoskeleton(
    project_name='Exoskeleton Validation Test',
    database_settings={'port': DB_PORT,
                       'database': 'exoskeleton',
                       'username': 'exoskeleton',
                       'passphrase': 'exoskeleton'},
    bot_behavior={'queue_max_retries': 6,
                  'wait_min': 1,
                  'wait_max': 5,
                  'connection_timeout': 30,
                  'queue_revisit': 5,
                  'stop_if_queue_empty': True},
    filename_prefix='EXO_',
    chrome_name=BROWSER,
    target_directory='./fileDownloads'
)


logging.info('Change constants for automatic test')
# If there is a temporary error, there are delays up
# to 6 hours between new tries. To test this without
# astronomical runtimes, change that:
exo.errorhandling.DELAY_TRIES = (1, 1, 1, 1, 1)
delay_steps = f"DELAY_TRIES: {exo.errorhandling.DELAY_TRIES}"
logging.info(delay_steps)

# #############################################################################
# HELPER FUNCTIONS FOR VARIOUS TESTS
# #############################################################################


logging.info('Define helper functions')


def queue_count() -> int:
    "Return the number of items in the queue that are not blocked as errors"
    stats = exo.stats.queue_stats()
    return stats['tasks_without_error']


def label_count() -> int:
    "Check if the number of labels equals the expected number."
    exo.cur.execute('SELECT COUNT(*) FROM exoskeleton.labels;')
    labelcount = exo.cur.fetchone()
    return int(labelcount[0]) if labelcount else 0


def check_error_codes(expectation: set):
    exo.cur.execute('SELECT causesError FROM queue ' +
                    'WHERE causesError IS NOT NULL ' +
                    'ORDER BY causesError;')
    error_codes = exo.cur.fetchall()
    error_codes = {(c[0]) for c in error_codes}
    if error_codes == expectation:
        logging.info('Error codes match expectation.')
    else:
        raise Exception("Wrong error codes found in the queue: " +
                        f"{error_codes} instead of {expectation}")


def filemaster_labels_by_url(url: str) -> set:
    """Primary use for automatic test:
        Get a list of label names (not id numbers!) attached to a specific
        filemaster entry via its URL instead of the id.
        The reason for this: The association with the URL predates
        the filemaster entry / the id."""
    # TO DO: clearer description!
    exo.cur.execute('SELECT DISTINCT shortName ' +
                    'FROM labels WHERE ID IN (' +
                    'SELECT labelID FROM labelToMaster ' +
                    'WHERE urlHash = SHA2(%s,256));', (url, ))
    labels = exo.cur.fetchall()
    return {(label[0]) for label in labels} if labels else set()  # type: ignore[index]


logging.info('Define counters etc')

# to track changes, track expectations:
test_counter = Counter()

# Check database state before running tests:
assert queue_count() == 0, "Database / Queue is not empty at test-start"
assert label_count() == 0, "Database / Table labels is not empty at test-start"


# #############################################################################
# ExoUrl class
# #############################################################################
@pytest.mark.parametrize("url", [
    ('https://www.example.com'),
    ('https://github.com/RuedigerVoigt/exoskeleton')
])
def test_exo_url_generate_sha256_hash(url: str):
    hash_python = exo_url.ExoUrl(url).hash
    exo.cur.execute('SELECT SHA2(%s, 256);', (url, ))
    hash_db = exo.cur.fetchone()[0]
    assert hash_python == hash_db


# #############################################################################
# TEST ADDING AND REMOVING TASKS TO THE QUEUE
# #############################################################################


def test_add_file_download():
    # unsupported protocol
    with pytest.raises(ValueError) as excinfo:
        _ = exo.add_file_download('ftp://www.ruediger-voigt.eu/')
    assert 'invalid or unsupported' in str(excinfo.value)
    # standard add
    uuid_1 = exo.add_file_download(
        'https://www.ruediger-voigt.eu/examplefile.txt')
    assert uuid_1 is not None, 'File download was not added'
    exo.delete_from_queue(uuid_1)
    # check with exoUrl
    uuid_2 = exo.add_file_download(exo_url.ExoUrl('https://www.example.com/exo-url-test.html'))
    assert uuid_2 is not None, 'File download was not added'
    exo.delete_from_queue(uuid_2)


def test_add_file_download_FORCE_NEW_VERSION():
    uuid_1 = exo.add_file_download(
        'https://www.ruediger-voigt.eu/force-new-version.txt')
    assert uuid_1 is not None
    # now force downloading a second version
    uuid_2 = exo.add_file_download(
        'https://www.ruediger-voigt.eu/force-new-version.txt', None, None, True)
    assert uuid_2 is not None, 'Second version was not forced, but should be'
    # clean up:
    exo.delete_from_queue(uuid_1)
    exo.delete_from_queue(uuid_2)


def test_add_save_page_code():
    # malformed URL: must not be added to the queue
    with pytest.raises(ValueError) as excinfo:
        _ = exo.add_save_page_code('missingschema.example.com')
    assert 'Malformed' in str(excinfo.value)
    # add task
    uuid_1 = exo.add_save_page_code('https://www.ruediger-voigt.eu/')
    exo.delete_from_queue(uuid_1)
    # add task with ExoUrl
    uuid_2 = exo.add_save_page_code(exo_url.ExoUrl('https://www.ruediger-voigt.eu/'))
    exo.delete_from_queue(uuid_2)


def test_add_save_page_text():
    # standard call
    uuid_1 = exo.add_save_page_text('https://www.ruediger-voigt.eu/examplefile.txt')
    exo.delete_from_queue(uuid_1)
    # standard call with ExoUrl
    uuid_2a = exo.add_save_page_text(exo_url.ExoUrl('https://www.ruediger-voigt.eu/examplefile.txt'))
    # force adding a second version of the same task
    uuid_2b = exo.add_save_page_text(exo_url.ExoUrl('https://www.ruediger-voigt.eu/examplefile.txt'), None, None, True)
    assert uuid_2b is not None, 'Did not force downloading page text'
    # clean up
    exo.delete_from_queue(uuid_2a)
    exo.delete_from_queue(uuid_2b)


def test_add_page_to_pdf():
    uuid_1 = exo.add_page_to_pdf('https://www.example.com')
    uuid_2 = exo.add_page_to_pdf(exo_url.ExoUrl('https://www.example.com'))
    # clean up
    exo.delete_from_queue(uuid_1)
    exo.delete_from_queue(uuid_2)


# #############################################################################
# MAIN CLASS
# #############################################################################


def test_random_wait():
    # check the call of random_wait from main.
    with patch('time.sleep', return_value=None):
        exo.random_wait()


def test_stats_module_counters():
    assert exo.stats.cnt['processed'] == 0
    exo.stats.increment_processed_counter()
    assert exo.stats.cnt['processed'] == 1
    assert exo.stats.get_processed_counter() == 1


def test_add_to_queue():
    "Under normal circumstances this function is not called directly"
    before = queue_count()
    # Try to add an unknown action type to the queue
    with pytest.raises(ValueError):
        exo.queue.add_to_queue('https://www.example.com', 99999)
    # Nothing should have been added to the queue:
    assert queue_count() == before, 'Unknown action added to the queue'


def test_add_same_task_with_different_labels():
    """Try to add the same task with different labels.
       One filemaster label is duplicate (but should show up only once)
       Expected behavior:
       * no new version / task is created.
       * new labels are added to the filemaster entry, but already
         existing ones are ignored
       * version labels are not added at all"""
    before = queue_count()
    # Add to queue "save a page as PDF"
    filemaster_labels_1 = {'i1fml1', 'i1fml2'}
    version_labels_1 = {'i1vl1', 'i1vl2', 'i1vl3'}
    test_url = 'https://www.google.com'
    url_hash_1 = hashlib.sha256(test_url.encode('utf-8')).hexdigest()
    uuid_1 = exo.add_page_to_pdf(test_url,
                                 filemaster_labels_1,
                                 version_labels_1)
    assert queue_count() == before + 1
    test_counter['num_expected_labels'] += 5

    # Check if all the labels were added:
    assert exo.labels.version_labels_by_uuid(uuid_1) == version_labels_1
    assert filemaster_labels_by_url(test_url) == filemaster_labels_1

    filemaster_labels_2 = {'i1fml2', 'i1fml3'}
    version_labels_to_be_ignored = {'ignore_me1', 'ignore_me2'}
    # the URL is not added, but a label:
    uuid_2 = exo.add_page_to_pdf(test_url,
                                 filemaster_labels_2,
                                 version_labels_to_be_ignored)
    assert uuid_2 is None, "uuid_item: falsely added item to the queue!"
    test_counter['num_expected_labels'] += 1

    expected_fm_labels = filemaster_labels_1 | filemaster_labels_2
    # The same filemaster entry as the item before:
    assert filemaster_labels_by_url(test_url) == expected_fm_labels

    # See if version labels for item 2 are not in the label list by counting:
    all_added_labels = (filemaster_labels_1 | version_labels_1 |
                        filemaster_labels_2)

    assert label_count() == len(all_added_labels)


def get_filemaster_id():
    # Add a task to the queue
    test_url = 'https://www.example.com/get-fm-id.html'
    test_uuid = exo.add_page_to_pdf(test_url)
    # Get the filemaster ID
    fm_id = exo.labels.get_filemaster_id(test_uuid)
    # Get the URL via the id
    exo.cur.execute("SELECT url FROM fileMaster WHERE id = %s;", (fm_id, ))
    url_in_db = exo.cur.fetchone()[0]
    # Compare
    assert test_url == url_in_db
    # clean up
    exo.delete_from_queue(test_uuid)
    # query with a bogus version uuid
    with pytest.raises(ValueError) as excinfo:
        exo.labels.get_filemaster_id('bogus')
    assert "Invalid" in str(excinfo.value)


def test_queue_get_filemaster_id_by_url():
    # Add a task to the queue
    test_url = 'https://www.example.com/get-fm-id-by-url.html'
    test_uuid = exo.add_page_to_pdf(test_url)
    # Compare results of different methods to get the id
    assert exo.labels.get_filemaster_id(test_uuid) == exo.queue.get_filemaster_id_by_url(test_url)
    # clean up
    exo.delete_from_queue(test_uuid)


def test_filemaster_labels_by_url():
    # Add a task with filemaster labels
    m_labels = {'test_fm_labels_by_url_1', 'test_fm_labels_by_url_2'}
    test_url = 'https://www.example.com/fm-labels-by-url.html'
    test_uuid = exo.add_page_to_pdf(test_url, labels_master=m_labels)
    assert exo.labels.filemaster_labels_by_url(test_url) == m_labels
    # clean up
    exo.delete_from_queue(test_uuid)
    test_counter['num_expected_labels'] += 2


def test_version_labels_by_uuid():
    # Add a task with version labels
    v_labels = {'test_version_labels_by_uuid_1', 'test_version_labels_by_uuid_2'}
    test_uuid = exo.add_page_to_pdf(
        'https://www.example.com/version-labels.html',
        labels_version=v_labels)
    # Pull the version labels assigned and compare them:
    assert exo.labels.version_labels_by_uuid(test_uuid) == v_labels
    # clean up
    exo.delete_from_queue(test_uuid)
    test_counter['num_expected_labels'] += 2


def test_assign_labels_to_master():
    # Add a task without any filemaster label
    test_url = 'https://www.example.com/assign-label-to-fm.html'
    test_uuid = exo.add_page_to_pdf(test_url)
    # test guard
    exo.labels.assign_labels_to_master(test_url, set()) is None
    exo.labels.assign_labels_to_master(test_url, None) is None
    # Now use the URL to add some labels at filemaster level
    fm_labels = {'assign_to_fm_test_1', 'assign_to_fm_test_2'}
    exo.labels.assign_labels_to_master(test_url, fm_labels)
    # pull the labels and compare them
    assert exo.labels.filemaster_labels_by_url(test_url) == fm_labels
    # repeat with ExoUrl
    assert exo.labels.filemaster_labels_by_url(exo_url.ExoUrl(test_url)) == fm_labels
    # clean up
    exo.delete_from_queue(test_uuid)
    test_counter['num_expected_labels'] += 2


# def test_all_labels_by_uuid():
#     # Add a task with labels for filemaster and version!
#     test_url = 'https://www.example.com/all-labels.html'
#     test_uuid = exo.add_page_to_pdf(
#         test_url,
#         labels_master={'test_all_labels_1', 'test_all_labels_2'},
#         labels_version={'test_all_labels_3'})
#     expected = {'test_all_labels_1', 'test_all_labels_2', 'test_all_labels_3'}
#     assert len(exo.labels.version_labels_by_uuid(test_uuid)) == 1
#     exo.cur.execute('SELECT url FROM fileMaster WHERE id = %s;', (exo.labels.get_filemaster_id(test_uuid), ))
#     filemaster_url = exo.cur.fetchone()
#     assert filemaster_url == test_url
#     assert exo.labels.all_labels_by_uuid(test_uuid) == expected
#     # clean up
#     exo.delete_from_queue(test_uuid)
#     test_counter['num_expected_labels'] += 3


def test_remove_labels_from_uuid():
    # Add a task without any filemaster label
    test_url = 'https://www.example.com/assign-label-to-fm.html'
    test_labels = {'remove_me_1', 'remove_me_2'}
    uuid_1 = exo.add_page_to_pdf(test_url, labels_version=test_labels)
    test_counter['num_expected_labels'] += 2
    exo.labels.remove_labels_from_uuid(uuid_1, test_labels)
    # TO DO: how many labels are attached?
    exo.delete_from_queue(uuid_1)


def test_version_uuids_by_label():
    # guard clause catches unknown label:
    with pytest.raises(ValueError) as excinfo:
        exo.labels.version_uuids_by_label("never used")
    assert "Unknown label" in str(excinfo.value)
    # Add a task with label
    test_uuid = exo.add_page_to_pdf('https://www.example.com/uuid_by_label.html',
                                    labels_version={'uuid_by_label_check'})
    # Searching with the label should return a set with one item
    assert exo.labels.version_uuids_by_label('uuid_by_label_check') == {test_uuid}
    # Switch to only processed items
    assert exo.labels.version_uuids_by_label('uuid_by_label_check', processed_only=True) == set()
    # clean up
    exo.delete_from_queue(test_uuid)
    # TO DO: function to remove unused labels from db
    test_counter['num_expected_labels'] += 1


def test_define_or_update_label():
    "Try to add an invalid label"
    before = label_count()
    exo.labels.define_or_update_label('foo'*33, 'labels are limited to 31 chars')
    # implemented behavior is to just return without exception
    assert label_count() == before
    exo.labels.define_or_update_label('some random', '')
    test_counter['num_expected_labels'] += 1
    assert label_count() == before + 1


def test_get_label_ids():
    # at first define some labels
    exo.labels.define_or_update_label('yet another label', '')
    exo.labels.define_or_update_label('yet another label2', '')
    test_counter['num_expected_labels'] += 2
    # ask for their ids
    ids = exo.labels.get_label_ids({'yet another label', 'yet another label2'})
    assert len(ids) == 2


def test_get_label_ids_EMPTY_SET(caplog):
    assert exo.labels.get_label_ids(set()) == set()
    assert 'No labels provided' in caplog.text


def test_same_url_different_task():
    """Add the same URL to the queue, but with a different task.
       Then add the same new label to filemaster and version.
        Expected behavior:
        * new filemaster labels should be attached to the already
          existing filemaster
        * version labels should be attached to this specific version"""

    before = queue_count()

    uuid_t1_3 = exo.add_save_page_code(
        'https://www.google.com', {'item3_label'}, {'item3_label'})

    assert queue_count() == before + 1
    test_counter['num_expected_labels'] += 1
    assert label_count() == test_counter['num_expected_labels']
    assert exo.labels.version_labels_by_uuid(uuid_t1_3) == {'item3_label'}


def test_remove_task_keep_labels():
    """Add new task to the queue with unique labels.
       Remove it from the queue before it is executed.
       Expectation:
       - the task is removed
       - the labels are just dettached and NOT removed"""
    before = queue_count()
    uuid_t1_8 = exo.add_save_page_code(
        'https://www.example.com',
        {'unique_fm_label'},
        {'unique_version_label'})
    assert queue_count() == before + 1
    test_counter['num_expected_labels'] += 2
    assert label_count() == test_counter['num_expected_labels']
    # now remove it again
    exo.delete_from_queue(uuid_t1_8)
    assert queue_count() == before
    # this does not change the number of labels:
    assert label_count() == test_counter['num_expected_labels']


def test_return_page_code():
    exo.return_page_code('https://www.ruediger-voigt.eu/')
    exo.return_page_code(exo_url.ExoUrl('https://www.ruediger-voigt.eu/'))
    with pytest.raises(ValueError) as excinfo:
        exo.return_page_code(None)
    assert 'Missing URL' in str(excinfo.value)
    with pytest.raises(RuntimeError) as excinfo:
        exo.return_page_code("https://www.ruediger-voigt.eu/throw-402.html")
    assert 'Cannot return page code' in str(excinfo.value)

# TO Do: handle 404 separetly


def test_process_queue():
    exo.process_queue()
    # check_queue_count returns the number of items which did *not* cause permanent errors
    assert queue_count() == 0, 'Did not process everything it should have'


# #############################################################################
# TEST BLOCKLIST FEATURE
# #############################################################################

def test_block_unblock():
    """Task already in the queue when its host is added to the blocklist.
    Expectations:
    - Queue item is not removed.
    - Action will not be started as the blocklist is checked."""
    before = queue_count()
    # Add another task to the queue
    blocked_task_uuid = exo.add_save_page_code('https://www.github.com')
    # NOW set the host on the blocklist
    exo.blocklist.block_fqdn('www.github.com')
    # task remains
    assert queue_count() == before + 1, 'Task removed after host added to blocklist'
    # Add the same FQDN to blocklist (is ignored)
    exo.blocklist.block_fqdn('www.github.com')
    # unblock it again
    exo.blocklist.unblock_fqdn('www.github.com')
    # empty the queue completly
    exo.delete_from_queue(blocked_task_uuid)


def test_blocklist_too_long_fqdn():
    "try to add an invalid FQDN (longer than allowed by standard)"
    with pytest.raises(ValueError) as excinfo:
        exo.blocklist.block_fqdn('foo'*255)
    assert 'Not a valid FQDN' in str(excinfo.value)


def test_remove_from_blocklist():
    before = queue_count()
    # Add host to blocklist
    exo.blocklist.block_fqdn('www.google.com')
    # try to add URL with blocked FQDN
    with pytest.raises(err.HostOnBlocklistError):
        _ = exo.add_page_to_pdf(
            'https://www.google.com/search?q=exoskeleton+python',
            {'label_that_should-be_ignored'},
            {'another_label_to_ignore'})
    assert queue_count() == before, 'URL added to queue even though host on blocklist'
    assert label_count() == test_counter['num_expected_labels']
    # Remove the fqdn from the blocklist.
    # Add the previously blocked URL with a task
    exo.blocklist.unblock_fqdn('www.google.com')
    exo.add_save_page_code(
        'https://www.google.com/search?q=exoskeleton+python')
    test_counter['num_expected_labels'] += 0
    assert queue_count() == before + 1
    assert label_count() == test_counter['num_expected_labels']

    # process the queue
    exo.process_queue()

    # check_queue_count returns the number of items
    # which did *not* cause permanent errors
    test_counter['num_expected_queue_items'] = 0
    assert queue_count() == test_counter['num_expected_queue_items']

    exo.cur.execute('SELECT COUNT(*) FROM queue WHERE causesError IS NOT NULL;')
    permanent_errors = int(exo.cur.fetchone()[0])

    assert permanent_errors == 0


# #############################################################################
# TEST ERROR HANDLING
# #############################################################################


def test_exceed_retries():
    # The server is configured to *always* return the error
    # code named in the URL.
    # Add temporary error
    uuid_code_500 = exo.add_save_page_code(
        "https://www.ruediger-voigt.eu/throw-500.html")
    # Add permanent errors
    exo.add_save_page_code("https://www.ruediger-voigt.eu/throw-402.html")
    exo.add_save_page_code("https://www.ruediger-voigt.eu/throw-404.html")
    exo.add_save_page_code("https://www.ruediger-voigt.eu/throw-407.html")
    exo.add_save_page_code("https://www.ruediger-voigt.eu/throw-410.html")
    exo.add_save_page_code("https://www.ruediger-voigt.eu/throw-451.html")
    # Update counter
    test_counter['num_expected_queue_items'] += 6
    # Start processing:
    exo.process_queue()
    exo.cur.execute('SELECT causesError, numTries FROM queue WHERE id = %s;',
                    uuid_code_500)
    error_description = exo.cur.fetchone()
    assert error_description == (3, 6), f"Wrong error for exceeded retries: {error_description}"


def test_forget_errors():
    check_error_codes({3, 451, 402, 404, 407, 410})
    exo.errorhandling.forget_specific_error(404)
    check_error_codes({3, 451, 402, 407, 410})
    exo.errorhandling.forget_permanent_errors()
    check_error_codes(set())
    exo.errorhandling.forget_temporary_errors()
    # process the queue again
    exo.process_queue()
    # Check they all are marked as errors again:
    check_error_codes({3, 451, 402, 404, 407, 410})
    # Now remove all errors
    exo.errorhandling.forget_all_errors()
    check_error_codes(set())
    # Truncate the queue
    exo.cur.execute('TRUNCATE TABLE queue;')


def test_log_temporary_problem():
    test_url = 'https://www.example.com'
    exo.stats.log_temporary_problem(exo_url.ExoUrl(test_url))
    exo.errorhandling.forget_temporary_errors()


def test_log_rate_limit_hit():
    test_url = 'https://www.example.com'
    exo.stats.log_rate_limit_hit(exo_url.ExoUrl(test_url))
    exo.errorhandling.forget_permanent_errors()


# #############################################################################
# TEST HANDLING REDIRECTS
# #############################################################################


def test_handle_redirects():
    "Add some URLs that redirect"
    # permanently moved:
    redirect301 = exo.add_save_page_code(
        "https://www.ruediger-voigt.eu/redirect-301.html")
    # temporarily moved:
    redirect302 = exo.add_save_page_code(
        "https://www.ruediger-voigt.eu/redirect-302.html")
    exo.process_queue()

    exo.cur.execute('SELECT pageContent FROM fileContent WHERE versionID =%s;',
                    redirect301)
    filecontent_301 = (exo.cur.fetchone())[0]
    assert filecontent_301 == 'testfile1', 'Redirect 301 did not work.'
    exo.cur.execute('SELECT pageContent FROM fileContent WHERE versionID =%s;',
                    redirect302)
    filecontent_302 = (exo.cur.fetchone())[0]
    assert filecontent_302 == 'testfile2', 'Redirect 302 did not work.'


# #############################################################################
# TEST RATE LIMIT
# #############################################################################

logging.info('Test 6: Rate Limit')


def count_rate_limit() -> int:
    exo.cur.execute('SELECT COUNT(*) FROM rateLimits;')
    count = int((exo.cur.fetchone())[0])
    return count


def test_hit_a_rate_limit():
    # Add a single rate limit:
    exo.errorhandling.add_rate_limit('www.example.com')
    assert count_rate_limit() == 1, 'Did not add rate limit!'
    # Forget this rate limit:
    exo.errorhandling.forget_specific_rate_limit('www.example.com')
    assert count_rate_limit() == 0, 'Did not remove rate limit!'
    # Add two rate limits, but duplicate one.
    exo.errorhandling.add_rate_limit('www.example.com')
    exo.errorhandling.add_rate_limit('www.example.com')
    exo.errorhandling.add_rate_limit('www.ruediger-voigt.eu')
    assert count_rate_limit() == 2, 'Duplicate rate limit'
    logging.info('Forget all rate limits')
    exo.errorhandling.forget_all_rate_limits()
    assert count_rate_limit() == 0, 'Did not forget rate limits'
    # Check that a rate limited FQDN does not show up as next item.
    exo.errorhandling.add_rate_limit('www.ruediger-voigt.eu')
    exo.cur.execute('TRUNCATE TABLE queue;')
    exo.add_save_page_code('https://www.ruediger-voigt.eu/throw-429.html')
    assert exo.queue.get_next_task() is None, 'Rate limited task showed up as next'
    exo.errorhandling.forget_all_rate_limits()

# #############################################################################
# TEST JOB MANAGER
# #############################################################################


def test_job_manager():
    exo.jobs.define_new('Example Job', 'https://www.example.com')
    # Define the job again with the same parameters
    # which is ignored except for a log entry
    exo.jobs.define_new('Example Job', 'https://www.example.com')
    # Job name too long:
    with pytest.raises(ValueError):
        exo.jobs.define_new('foo' * 127, 'https://www.example.com')
    # try to define job with same name but different start url
    with pytest.raises(ValueError):
        exo.jobs.define_new('Example Job', 'https://www.example.com/foo.html')
    # Missing required job parameters
    with pytest.raises(ValueError):
        exo.jobs.define_new('Example Job', '')
    with pytest.raises(ValueError):
        exo.jobs.define_new('', 'https://www.example.com')
    # Update the URL
    exo.jobs.update_current_url('Example Job', 'https://www.example.com/bar.html')
    with pytest.raises(ValueError):
        exo.jobs.update_current_url(None, 'https://www.example.com/bar.html')
    with pytest.raises(ValueError):
        exo.jobs.update_current_url('Example Job', None)
    with pytest.raises(ValueError):
        exo.jobs.update_current_url('Unknown Job', 'https://www.example.com/')
    # Get the URL
    assert exo.jobs.get_current_url('Example Job') == 'https://www.example.com/bar.html'
    with pytest.raises(ValueError):
        exo.jobs.get_current_url('Unknown Job')
    # mark a job as finished
    exo.jobs.mark_as_finished('Example Job')
    with pytest.raises(ValueError):
        exo.jobs.mark_as_finished('   ')
    with pytest.raises(ValueError):
        exo.jobs.mark_as_finished(None)
    # try to get the current URL of a finished job
    with pytest.raises(RuntimeError):
        exo.jobs.get_current_url('Example Job')
    # Use methods with ExoUrl object instead of URL string
    exo.jobs.define_new('ExoUrl Job', exo_url.ExoUrl('https://www.example.com/exourl.html'))
    exo.jobs.update_current_url('ExoUrl Job', exo_url.ExoUrl('https://www.example.com/exourl-2.html'))
    exo.jobs.mark_as_finished('ExoUrl Job')


# try to change the current URL of a finished job
# TO DO
# exo.jobs.update_current_url('Example Job', 'https://www.github.com/')


# #############################################################################
# TEST NOTIFICATIONS
# #############################################################################


# #############################################################################
# TEST CONTROLLED BROWSER
# #############################################################################


def test_page_to_pdf_EXCEPTIONS():
    with patch('subprocess.run', side_effect=subprocess.TimeoutExpired):
        exo.controlled_browser.page_to_pdf(
            url=exo_url.ExoUrl('https://www.ruediger-voigt.eu'),
            file_path='./fileDownloads/setup-to-fail.pdf',
            queue_id='foo')
    with patch('subprocess.run', side_effect=subprocess.CalledProcessError):
        exo.controlled_browser.page_to_pdf(
            url=exo_url.ExoUrl('https://www.ruediger-voigt.eu'),
            file_path='./fileDownloads/setup-to-fail.pdf',
            queue_id='foo')


# ############################################
# Test: Clean Up
# ############################################


def test_clean_up_functions():
    exo.blocklist.truncate_blocklist()


# #############################################################################
# TEST DATABASE CONNECTION
# #############################################################################

def test_establish_db_connection_OPERATIONAL_ERROR(caplog):
    with patch('pymysql.connect', side_effect=pymysql.OperationalError):
        with pytest.raises(pymysql.OperationalError):
            exo.db.establish_db_connection()
    assert 'Did you forget a parameter' in caplog.text


def test_establish_db_connection_INTERFACE_ERROR(caplog):
    with patch('pymysql.connect', side_effect=pymysql.InterfaceError):
        with pytest.raises(pymysql.InterfaceError):
            exo.db.establish_db_connection()
    assert 'Database related exception' in caplog.text


def test_establish_db_connection_DATABASE_ERROR(caplog):
    with patch('pymysql.connect', side_effect=pymysql.InterfaceError):
        with pytest.raises(pymysql.InterfaceError):
            exo.db.establish_db_connection()
    assert 'Database related exception' in caplog.text


def test_establish_db_connection_CATCHALL_ERROR_1(caplog):
    with patch('pymysql.connect', side_effect=pymysql.Error):
        with pytest.raises(pymysql.Error):
            exo.db.establish_db_connection()
    assert 'Exception while connecting' in caplog.text


def test_establish_db_connection_CATCHALL_ERROR_2(caplog):
    with patch('pymysql.connect', side_effect=Exception):
        with pytest.raises(Exception):
            exo.db.establish_db_connection()
    assert 'Exception while connecting' in caplog.text

# #############################################################################
# TEST SCHEMA CHECK
# #############################################################################


def test_schema_check():
    # Mess up the tables with expected elements to provoke exceptions:
    exo.db_check.PROCEDURES.append('foo')
    exo.db_check.FUNCTIONS.append('foo')
    exo.db_check.TABLES.append('foo')
    # See if there are exceptions
    with pytest.raises(err.InvalidDatabaseSchemaError):
        exo.db_check._DatabaseSchemaCheck__check_stored_procedures()
    with pytest.raises(err.InvalidDatabaseSchemaError):
        exo.db_check._DatabaseSchemaCheck__check_functions()
    with pytest.raises(err.InvalidDatabaseSchemaError):
        exo.db_check._DatabaseSchemaCheck__check_table_existence()


# #############################################################################
# CREATE INSTANCES WITH DIFFERENT PARAMETERS
# #############################################################################


def test_no_host_no_port_no_pw():
    """The parameters host, port, and password have defaults.
       However that will yield an exception as the database
       service requires a password."""
    with pytest.raises(pymysql.OperationalError):
        no_host_no_port_no_pass = exoskeleton.Exoskeleton(
            project_name='Exoskeleton Validation Test',
            database_settings={'database': 'exoskeleton',
                               'username': 'exoskeleton'},
            filename_prefix='EXO_',
            target_directory='./fileDownloads'
        )


with pytest.raises(ValueError) as excinfo:
    non_existent_browser = exoskeleton.Exoskeleton(
        project_name='Exoskeleton Validation Test',
        database_settings={'port': DB_PORT,
                           'database': 'exoskeleton',
                           'username': 'exoskeleton',
                           'passphrase': 'exoskeleton'},
        filename_prefix='EXO_',
        chrome_name='unknown',
        target_directory='./fileDownloads'
    )
assert 'not in path' in str(excinfo.value)


with pytest.raises(ValueError) as excinfo:
    milestone_non_numeric = exoskeleton.Exoskeleton(
        project_name='Exoskeleton Validation Test',
        database_settings={'port': DB_PORT,
                           'database': 'exoskeleton',
                           'username': 'exoskeleton',
                           'passphrase': 'exoskeleton'},
        filename_prefix='EXO_',
        target_directory='./fileDownloads',
        mail_behavior={
            'send_start_msg': True,
            'send_finish_msg': True,
            'milestone_num': 'abc'}
    )
assert 'milestone_num must be integer' in str(excinfo.value)


# Valid mail_settings and mail_behavior
# will not try to send mail until it is called after the constructor

valid_mail = exoskeleton.Exoskeleton(
    project_name='Exoskeleton Validation Test',
    database_settings={'port': DB_PORT,
                       'database': 'exoskeleton',
                       'username': 'exoskeleton',
                       'passphrase': 'exoskeleton'},
    filename_prefix='EXO_',
    target_directory='./fileDownloads',
    mail_settings={
        'server': 'smtp.example.com',
        'server_port': 587,
        'encryption': 'starttls',
        'username': 'pytest',
        'passphrase': 'example',
        'recipient': 'test@example.com',
        'sender': 'pytest@example.com'},
    mail_behavior={
        'send_start_msg': False,
        'send_finish_msg': True,
        'milestone_num': 3}
)

# #############################################################################

logging.info('Test: no browser set, but add task that require one')

no_browser = exoskeleton.Exoskeleton(
    project_name='Exoskeleton Validation Test',
    database_settings={'port': DB_PORT,
                       'database': 'exoskeleton',
                       'username': 'exoskeleton',
                       'passphrase': 'exoskeleton'},
    filename_prefix='EXO_',
    target_directory='./fileDownloads'
)

no_browser.add_page_to_pdf('https://www.example.com/foo123.html')


# ############################################
# DONE
# ############################################

print('Passed all tests. Done!')
