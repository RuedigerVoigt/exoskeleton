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
coverage run --source exoskeleton -m pytest tests_with_sideffects.py
To generate a report limited to that run afterwards:
coverage html
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2021 Rüdiger Voigt
Released under the Apache License 2.0
"""

import hashlib
import logging
from unittest.mock import patch

logging.basicConfig(level=logging.DEBUG)

import pytest

import exoskeleton
from exoskeleton import actions
from exoskeleton import database_connection
from exoskeleton import file_manager
from exoskeleton import notification_manager
from exoskeleton import remote_control_chrome
from exoskeleton import statistics_manager
from exoskeleton import time_manager


# #############################################################################
# TESTS THAT NEED AN INSTANCE OF EXOSKELETON
# #############################################################################

logging.info('Create an instance')

DB_PORT = 12345
BROWSER = 'chromium-browser'

exo = exoskeleton.Exoskeleton(
    project_name='Exoskeleton Validation Test',
    database_settings={'port': DB_PORT,
                       'database': 'exoskeleton',
                       'username': 'exoskeleton',
                       'passphrase': 'exoskeleton'},
    bot_behavior={'queue_max_retries': 5,
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

logging.info('Define helper functions')


def queue_count() -> int:
    "Return the number of items in the queue that are not blocked as errors"
    stats = exo.stats.queue_stats()
    return stats['tasks_without_error']


def label_count() -> int:
    "Check if the number of labels equals the expected number."
    exo.cur.execute('SELECT COUNT(*) FROM exoskeleton.labels;')
    return int(exo.cur.fetchone()[0])


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


# to track changes, track expectations:
num_expected_queue_items = 0
num_expected_labels = 0


logging.info('Check if the database is empty')
assert queue_count() == 0
assert label_count() == 0


# check the call of random_wait from main.
# That needs to be patched
with patch('time.sleep', return_value=None):
    exo.random_wait()


# check parts of the stats module
assert exo.stats.cnt['processed'] == 0
exo.stats.increment_processed_counter()
assert exo.stats.cnt['processed'] == 1
assert exo.stats.get_processed_counter() == 1


# #############################################################################

logging.info('Test: Try to add an unknown action type to the queue')

# Under normal circumstances this function is not called directly
exo.queue.add_to_queue('https://www.example.com', 99999)
# implemented behavior is not to raise an exception, but to return.
# => TO Do: maybe change that
# Nothing should have been added to the queue:
assert queue_count() == num_expected_queue_items

# #############################################################################

logging.info('Test: Add to queue "save a page as PDF')

# Add to queue "save a page as PDF"
t1_1_filemaster_labels = {'i1fml1', 'i1fml2'}
t1_1_version_labels = {'i1vl1', 'i1vl2', 'i1vl3'}
url_t1_1 = 'https://www.ruediger-voigt.eu/'
url_hash_t1_1 = hs = hashlib.sha256(url_t1_1.encode('utf-8')).hexdigest()
uuid_t1_1 = exo.add_page_to_pdf(url_t1_1,
                                t1_1_filemaster_labels,
                                t1_1_version_labels)
num_expected_queue_items += 1
num_expected_labels += 5

# Check if all the labels were added:
assert exo.version_labels_by_uuid(uuid_t1_1) == t1_1_version_labels
assert exo.filemaster_labels_by_url(url_t1_1) == t1_1_filemaster_labels

# #############################################################################

logging.info('Test: Try to add the same task with different labels.')

# Try to add the same task with different labels.
# One filemaster label is duplicate (but should show up only once)
# Expected behavior:
# * no new version / task is created.
# * new labels are added to the filemaster entry, but already
#   existing ones are ignored
# * version labels are not added at all

t1_2_filemaster_labels = {'i1fml2', 'i1fml3'}
t1_2_version_labels_to_be_ignored = {'ignore_me1', 'ignore_me2'}
uuid_t1_2 = exo.add_page_to_pdf(url_t1_1,
                                t1_2_filemaster_labels,
                                t1_2_version_labels_to_be_ignored)

num_expected_queue_items += 0
num_expected_labels += 1
assert queue_count() == num_expected_queue_items

if uuid_t1_2 is not None:
    raise Exception("uuid_item: falsely added item to the queue!")

expected_fm_labels = t1_1_filemaster_labels | t1_2_filemaster_labels
# The same filemaster entry as the item before:
assert exo.filemaster_labels_by_url(url_t1_1) == expected_fm_labels

# See if version labels for item 2 are not in the label list by counting:
all_added_labels = (t1_1_filemaster_labels | t1_1_version_labels |
                    t1_2_filemaster_labels)

assert label_count() == len(all_added_labels)

# #############################################################################

logging.info('Test: Try to add an invalid label.')

before = label_count()
exo.define_or_update_label('foo'*33, 'labels are limited to 31 chars')
# implemented behavior is to just return without exception
assert label_count() == before

# #############################################################################

logging.info('Test: add the same URL to the queue, but with a different task.')

# add the same URL to the queue, but with a different task.
# Add the same new label to filemaster and version.
# Expected behavior:
# * new filemaster labels should be attached to the already
#   existing filemaster
# * version labels should be attached to this specific version

uuid_t1_3 = exo.add_save_page_code(url_t1_1,
                                   {'item3_label'},
                                   {'item3_label'})

num_expected_queue_items += 1
num_expected_labels += 1

assert queue_count() == num_expected_queue_items
assert label_count() == num_expected_labels

assert exo.version_labels_by_uuid(uuid_t1_3) == {'item3_label'}

# #############################################################################

logging.info('Test: get the filemaster id by URL')

exo.get_filemaster_id_by_url(url_t1_1)


# #############################################################################

logging.info('Test: queue to download a file')

# queue to download a file
uuid_t1_4 = exo.add_file_download(
    'https://www.ruediger-voigt.eu/examplefile.txt')

num_expected_queue_items += 1
num_expected_labels += 0

assert queue_count() == num_expected_queue_items

# #############################################################################

logging.info('Test: force downloading a second version')

# force downloading a second version
uuid_t1_5 = exo.add_file_download(
    'https://www.ruediger-voigt.eu/examplefile.txt',
    None,
    None,
    True)

num_expected_queue_items += 1
num_expected_labels += 0

assert queue_count() == num_expected_queue_items

# #############################################################################

logging.info("Test: force downloading the page's text")

exo.add_save_page_text(
    'https://www.ruediger-voigt.eu/examplefile.txt',
    None,
    None,
    True)

num_expected_queue_items += 1
num_expected_labels += 0

assert queue_count() == num_expected_queue_items

# #############################################################################

logging.info('Test: block adding malformed URLs to the queue')

# malformed URL: must not be added to the queue
uuid_t1_6 = exo.add_save_page_code('missingschema.example.com')

num_expected_queue_items += 0
num_expected_labels += 0

assert queue_count() == num_expected_queue_items

# #############################################################################


# ################## TEST 1: TASK 7 ##################
# Add new task to the queue with unique labels.
# Remove the task from the queue before it is executed.
# Expectation:
# - the task is removed
# - the labels are just dettached and NOT removed too

logging.info('Test 1: Task 7')

uuid_t1_8 = exo.add_save_page_code(
    'https://www.example.com',
    {'unique_fm_label'},
    {'unique_version_label'})

num_expected_queue_items += 1
num_expected_labels += 2

assert queue_count() == num_expected_queue_items
assert label_count() == num_expected_labels

# now remove it again
logging.info('Test 1: Task 7: removing the item again')
exo.delete_from_queue(uuid_t1_8)

num_expected_queue_items -= 1
num_expected_labels += 0

assert queue_count() == num_expected_queue_items
assert label_count() == num_expected_labels

# ################## TEST 1: TASK 8 ##################

# just return the page's code.
exo.return_page_code('https://www.ruediger-voigt.eu/')
with pytest.raises(ValueError) as excinfo:
    exo.return_page_code(None)
assert 'Missing url' in str(excinfo.value)

# TO Do: handle 404 separetly

with pytest.raises(RuntimeError) as excinfo:
    exo.return_page_code("https://www.ruediger-voigt.eu/throw-402.html")
assert 'Cannot return page code' in str(excinfo.value)

# ################## TEST 1: FINAL ##################

logging.info('Test 1: final: process the queue')

# process the queue
exo.process_queue()

# check_queue_count returns the number of items
# which did *not* cause permanent errors
num_expected_queue_items = 0
assert queue_count() == num_expected_queue_items



# ############################################
# Test 2: Test the blocklist feature
# ############################################
logging.info('Starting Test 2: blocklist feature')

# ################## TEST 2: TASK 1 ##################

logging.info('Test 2: Task 1')

# Task already in the queue when its host is added to
# the blocklist. Expectation:
# - queue item is not removed
# - action will not be started as
#   the blocklist is checked

# Add another task to the queue
blocked_task_uuid = exo.add_save_page_code('https://www.github.com')
num_expected_queue_items += 1
num_expected_labels += 0

# NOW set the host on the blocklist
exo.block_fqdn('www.github.com')
# Add the same FQDN to blocklist (is ignored)
exo.block_fqdn('www.github.com')
# unblock it again
exo.unblock_fqdn('www.github.com')

# try to add an invalid FQDN (longer than allowed by standard)
with pytest.raises(ValueError) as excinfo:
    exo.block_fqdn('foo'*255)
assert 'No valid FQDN can be longer than 255' in str(excinfo.value)


# ################## TEST 2: TASK 2 ##################

logging.info('2.2: FQDN already on blocklist / remove from blocklist')

# Add host to blocklist
exo.block_fqdn('www.google.com')

# try to add URL with blocked FQDN
uuid_t1_7 = exo.add_page_to_pdf(
    'https://www.google.com/search?q=exoskeleton+python',
    {'label_that_should-be_ignored'},
    {'another_label_to_ignore'})
num_expected_queue_items += 0
num_expected_labels += 0

assert queue_count() == num_expected_queue_items
assert label_count() == num_expected_labels

# Remove the fqdn from the blocklist.
# Add the previously blocked URL with a task

exo.unblock_fqdn('www.google.com')

exo.add_save_page_code(
    'https://www.google.com/search?q=exoskeleton+python')
num_expected_queue_items += 1
num_expected_labels += 0

assert queue_count() == num_expected_queue_items
assert label_count() == num_expected_labels



# ################## TEST 2: FINAL ##################

logging.info('Test 2: final: process the queue')

# process the queue
exo.process_queue()

# check_queue_count returns the number of items
# which did *not* cause permanent errors
num_expected_queue_items = 0
assert queue_count() == num_expected_queue_items

exo.cur.execute('SELECT COUNT(*) FROM queue WHERE causesError IS NOT NULL;')
permanent_errors = int(exo.cur.fetchone()[0])

assert permanent_errors == 0

# empty the queue completly
exo.delete_from_queue(blocked_task_uuid)




# ############################################
# Test 3: Error Handling
# ############################################
logging.info('Starting Test 3: Error Handling')

# ################## TEST 3: TASK 1 ##################

logging.info('Test 3: Task 1: Repeated Temporary Errors')

# The server is configured to *always* return the error
# code named in the URL:
uuid_code_500 = exo.add_save_page_code(
    "https://www.ruediger-voigt.eu/throw-500.html")

num_expected_queue_items += 1

# Process together with the next task as otherwise the queue
# would be empty. This would increase wait times and make the
# test slower.
# exo.process_queue()

# ################## TEST 3: TASK 2 ##################

logging.info('Test 3: Task 2: Permanent Errors')

# The server is configured to always return the error
# code named in the URL:
exo.add_save_page_code("https://www.ruediger-voigt.eu/throw-402.html")
exo.add_save_page_code("https://www.ruediger-voigt.eu/throw-404.html")
exo.add_save_page_code("https://www.ruediger-voigt.eu/throw-407.html")
exo.add_save_page_code("https://www.ruediger-voigt.eu/throw-410.html")
exo.add_save_page_code("https://www.ruediger-voigt.eu/throw-451.html")

num_expected_queue_items += 5
num_expected_labels += 0

# ################## TEST 3: FINAL ##################

logging.info('3.final: Check the results')

exo.process_queue()

exo.cur.execute('SELECT causesError, numTries FROM queue WHERE id = %s;',
                uuid_code_500)
error_description = exo.cur.fetchone()

if error_description != (3, 5):
    raise Exception(f"Wrong error for exceeded retries: {error_description}")
else:
    logging.info("Correct error code for exceeded retries.")





# ############################################
# Test 4: Forgetting Errors
# ############################################

logging.info('Test 4: Forgetting Errors')

check_error_codes({3, 451, 402, 404, 407, 410})

logging.info('Forget a specific error')
exo.forget_specific_error(404)
check_error_codes({3, 451, 402, 407, 410})

logging.info('Forget all permanent errors')
exo.forget_permanent_errors()
check_error_codes(set())

logging.info('Forget temporary errors')
# TO Do add some beforehand
exo.forget_temporary_errors()

logging.info('process the queue again')
exo.process_queue()

logging.info('Check they all are marked as errors again')
check_error_codes({3, 451, 402, 404, 407, 410})

logging.info('Now remove all errors')
exo.forget_all_errors()
check_error_codes(set())

logging.info('Truncate the queue')
exo.cur.execute('TRUNCATE TABLE queue;')



# ############################################
# Test 5: Redirects
# ############################################

logging.info('Test 5: Redirects')

logging.info('Add some URLs that redirect')
# permanently moved:
redirect301 = exo.add_save_page_code(
    "https://www.ruediger-voigt.eu/redirect-301.html")
# temporarily moved:
redirect302 = exo.add_save_page_code(
    "https://www.ruediger-voigt.eu/redirect-302.html")

logging.info('Execute queue.')
exo.process_queue()

exo.cur.execute('SELECT pageContent FROM fileContent WHERE versionID =%s;',
                redirect301)
filecontent_301 = (exo.cur.fetchone())[0]
if filecontent_301 == 'testfile1':
    logging.info('Redirect 301 worked')
else:
    raise Exception('Redirect 301 DID NOT work.')

exo.cur.execute('SELECT pageContent FROM fileContent WHERE versionID =%s;',
                redirect302)
filecontent_302 = (exo.cur.fetchone())[0]
if filecontent_302 == 'testfile2':
    logging.info('Redirect 302 worked')
else:
    raise Exception('Redirect 302 DID NOT work.')



# ############################################
# Test 6: Hitting a Rate Limit
# ############################################

logging.info('Test 6: Rate Limit')


def count_rate_limit() -> int:
    exo.cur.execute('SELECT COUNT(*) FROM rateLimits;')
    count = int((exo.cur.fetchone())[0])
    return count


logging.info('Add a single rate limit')
exo.errorhandling.add_rate_limit('www.example.com')

if count_rate_limit() == 1:
    logging.info('Rate limit was added.')
else:
    raise Exception('Did not add rate limit!')

logging.info('Forget this rate limit')
exo.errorhandling.forget_specific_rate_limit('www.example.com')

if count_rate_limit() == 0:
    logging.info('Rate limit was removed.')
else:
    raise Exception('Did not remove rate limit!')

logging.info('Add two rate limits, but duplicate one.')
exo.errorhandling.add_rate_limit('www.example.com')
exo.errorhandling.add_rate_limit('www.example.com')
exo.errorhandling.add_rate_limit('www.ruediger-voigt.eu')
assert count_rate_limit() == 2

logging.info('Forget all rate limits')
exo.errorhandling.forget_all_rate_limits()
assert count_rate_limit() == 0

logging.info('Check that a rate limited FQDN does not show up as next item.')
exo.errorhandling.add_rate_limit('www.ruediger-voigt.eu')
exo.cur.execute('TRUNCATE TABLE queue;')
exo.add_save_page_code('https://www.ruediger-voigt.eu/throw-429.html')
print(exo.queue.get_next_task())
if exo.queue.get_next_task() is None:
    logging.info('As expected FQDN did not show up as next task')
else:
    raise Exception('Rate limited task showed up as next')
exo.errorhandling.forget_all_rate_limits()

# ############################################
# Test 7: Job Manager
# ############################################

logging.info('Test JobManager')
exo.job_define_new('Example Job', 'https://www.example.com')

# Define the job again with the same parameters
# which is ignored except for a log entry
exo.job_define_new('Example Job', 'https://www.example.com')
# Job name too long:
with pytest.raises(ValueError):
    exo.job_define_new('foo' * 127, 'https://www.example.com')

# try to define job with same name but different start url
with pytest.raises(ValueError):
    exo.job_define_new('Example Job', 'https://www.example.com/foo.html')

# Missing required job parameters
with pytest.raises(ValueError):
    exo.job_define_new('Example Job', '')
with pytest.raises(ValueError):
    exo.job_define_new('', 'https://www.example.com')

# Update the URL
exo.job_update_current_url('Example Job', 'https://www.example.com/bar.html')
with pytest.raises(ValueError):
    exo.job_update_current_url(None, 'https://www.example.com/bar.html')
with pytest.raises(ValueError):
    exo.job_update_current_url('Example Job', None)
with pytest.raises(ValueError):
    exo.job_update_current_url('Unknown Job', 'https://www.example.com/')
# Get the URL
exo.job_get_current_url('Example Job')
with pytest.raises(ValueError):
    exo.job_get_current_url('Unknown Job')

# mark a job as finished
exo.job_mark_as_finished('Example Job')
with pytest.raises(ValueError):
    exo.job_mark_as_finished('   ')
with pytest.raises(ValueError):
    exo.job_mark_as_finished(None)
# try to change the current URL of a finished job
# TO DO
# exo.job_update_current_url('Example Job', 'https://www.github.com/')

# try to get the current URL of a finished job
with pytest.raises(RuntimeError):
    exo.job_get_current_url('Example Job')


# ############################################
# Test 8: Clean Up
# ############################################

exo.truncate_blocklist()



# #############################################################################
# CREATE INSTANCES WITH DIFFERENT PARAMETERS
# #############################################################################

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
