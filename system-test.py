#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Automatic System Test for the exoskeleton crawler framework
# see: https://github.com/RuedigerVoigt/exoskeleton

import hashlib
import logging

import exoskeleton

logging.basicConfig(level=logging.DEBUG)

# ############################################
# Functions to test the database state
# ############################################


def check_queue_count(correct_number: int):
    u"""Check if the number of items in the queue equals
        the expected number. """
    found = exo.num_items_in_queue()
    if found != correct_number:
        raise Exception(f"Wrong number of items in the queue " +
                        f"({found} found / " +
                        f"{correct_number} expected).")
    else:
        logging.info('Number of queue items correct')


def check_label_count(correct_number: int):
    u"""Check if the number of labels equals the expected number."""
    exo.cur.execute('SELECT COUNT(*) FROM exoskeleton.labels;')
    found = int(exo.cur.fetchone()[0])

    if found != correct_number:
        raise Exception(f"Number of labels not correct" +
                        f"({found} found / " +
                        f"{correct_number} expected).")
    else:
        logging.info('Number of labels correct')

# ############################################
# SETUP
# ############################################


exo = exoskeleton.Exoskeleton(
    project_name='Exoskeleton Validation Test',
    database_settings={'port': 12345,
                       'database': 'exoskeleton',
                       'username': 'exoskeleton',
                       'passphrase': 'exoskeleton'},
    bot_behavior={'queue_max_retries': 5,
                  'wait_min': 1,
                  'wait_max': 5,
                  'connection_timeout': 30,
                  'stop_if_queue_empty': True},
    filename_prefix='EXO_',
    chrome_name='chromium-browser',
    target_directory='./fileDownloads'
)

num_expected_queue_items = 0
num_expected_labels = 0

# ############################################
# Check if database is empty
# ############################################

logging.info('Check if database is empty...')
check_queue_count(0)
check_label_count(0)

# ############################################
# Change constant for automatic test
# ############################################

# If there is a temporary error, there are delays up
# to 6 hours between new tries. To test this without
# astronomical runtimes, change that:

exo.DELAY_TRIES = (1, 1, 1, 1, 1)
delay_steps = f"DELAY_TRIES: {exo.DELAY_TRIES}"
logging.info(delay_steps)

# ############################################
# Test 1: Adding to the queue
# ############################################

# ################## TEST 1: TASK 1 ##################

logging.info('Test 1: Task 1')

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
if exo.version_labels_by_uuid(uuid_t1_1) != t1_1_version_labels:
    raise Exception(f"1.1: Version labels wrong for uuid {uuid_t1_1}!")
else:
    logging.info('1.1 version labels fine')


if exo.filemaster_labels_by_url(url_t1_1) != t1_1_filemaster_labels:
    raise Exception("1.1: Filemaster labels wrong")
else:
    logging.info('1.1 filemaster labels fine')

# ################## TEST 1: TASK 2 ##################
# Try to add the same task with different labels.
# One filemaster label is duplicate (but should show up only once)
# Expected behavior:
# * no new version / task is created.
# * new labels are added to the filemaster entry, but already
#   existing ones are ignored
# * version labels are not added at all

logging.info('Test 1: Task 2')

t1_2_filemaster_labels = {'i1fml2', 'i1fml3'}
t1_2_version_labels_to_be_ignored = {'ignore_me1', 'ignore_me2'}
uuid_t1_2 = exo.add_page_to_pdf(url_t1_1,
                                t1_2_filemaster_labels,
                                t1_2_version_labels_to_be_ignored)

num_expected_queue_items += 0
num_expected_labels += 1


check_queue_count(num_expected_queue_items)


if uuid_t1_2 is not None:
    raise Exception("uuid_item: falsely added item to the queue!")

expected_fm_labels = t1_1_filemaster_labels | t1_2_filemaster_labels
# The same filemaster entry as the item before:
if exo.filemaster_labels_by_url(url_t1_1) != expected_fm_labels:
    raise Exception("1.2: filemaster labels wrong!")
else:
    logging.info('1.2: filemaster labels fine')

# See if version labels for item 2 are not in the label list by counting:
all_added_labels = (t1_1_filemaster_labels | t1_1_version_labels |
                    t1_2_filemaster_labels)

check_label_count(len(all_added_labels))

# ################## TEST 1: TASK 3 ##################
# add the same URL to the queue, but with a different task.
# Add the same new label to filemaster and version.
# Expected behavior:
# * new filemaster labels should be attached to the already
#   existing filemaster
# * version labels should be attached to this specific version

logging.info('Test 1: Task 3')

uuid_t1_3 = exo.add_save_page_code(url_t1_1,
                                   {'item3_label'},
                                   {'item3_label'})

num_expected_queue_items += 1
num_expected_labels += 1

check_queue_count(num_expected_queue_items)
check_label_count(num_expected_labels)

if exo.version_labels_by_uuid(uuid_t1_3) != {'item3_label'}:
    raise Exception("1.3: wrong version labels")

# ################## TEST 1: TASK 4 ##################

logging.info('Test 1: Task 4')

# queue to download a file
uuid_t1_4 = exo.add_file_download(
    'https://www.ruediger-voigt.eu/examplefile.txt')

num_expected_queue_items += 1
num_expected_labels += 0

check_queue_count(num_expected_queue_items)

# ################## TEST 1: TASK 5 ##################

logging.info('Test 1: Task 5')

# force downloading a second version
uuid_t1_5 = exo.add_file_download(
    'https://www.ruediger-voigt.eu/examplefile.txt',
    None,
    None,
    True)

num_expected_queue_items += 1
num_expected_labels += 0

check_queue_count(num_expected_queue_items)

# ################## TEST 1: TASK 6 ##################

logging.info('Test 1: Task 6')

# malformed URL: must not be added to the queue
uuid_t1_6 = exo.add_save_page_code('missingschema.example.com')

num_expected_queue_items += 0
num_expected_labels += 0

check_queue_count(num_expected_queue_items)

# ################## TEST 1: TASK 7 ##################
# Add new task to the queue with unique labels.
# Remove the task from the queue before it is executed.
# Expectation:
# - the task is removed
# - the labels are just dettached and NOT removed too

logging.info('Test 1: Task 8')

uuid_t1_8 = exo.add_save_page_code(
    'https://www.example.com',
    {'unique_fm_label'},
    {'unique_version_label'})

num_expected_queue_items += 1
num_expected_labels += 2

check_queue_count(num_expected_queue_items)
check_label_count(num_expected_labels)

# now remove it again
logging.info('Test 1: Task 8: removing the item again')
exo.delete_from_queue(uuid_t1_8)

num_expected_queue_items -= 1
num_expected_labels += 0

check_queue_count(num_expected_queue_items)
check_label_count(num_expected_labels)

# ################## TEST 1: FINAL ##################

logging.info('Test 1: final: process the queue')

# Check one more time
logging.info("last checks befores processing starts..")
check_queue_count(num_expected_queue_items)
check_label_count(num_expected_labels)

# process the queue
exo.process_queue()

# check_queue_count returns the number of items
# which did *not* cause permanent errors
num_expected_queue_items = 0
check_queue_count(num_expected_queue_items)

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

check_queue_count(num_expected_queue_items)
check_label_count(num_expected_labels)

# Remove the fqdn from the blocklist.
# Add the previously blocked URL with a task

exo.unblock_fqdn('www.google.com')

exo.add_save_page_code(
    'https://www.google.com/search?q=exoskeleton+python')
num_expected_queue_items += 1
num_expected_labels += 0

check_queue_count(num_expected_queue_items)
check_label_count(num_expected_labels)



# ################## TEST 2: FINAL ##################

logging.info('Test 2: final: process the queue')

# process the queue
exo.process_queue()

# check_queue_count returns the number of items
# which did *not* cause permanent errors
num_expected_queue_items = 0
check_queue_count(num_expected_queue_items)

exo.cur.execute('SELECT COUNT(*) FROM queue WHERE causesError IS NOT NULL;')
permanent_errors = int(exo.cur.fetchone()[0])

if permanent_errors != 0:
    raise Exception('Wrong count of tasks with permanent errors.')

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
# Test 4
# ############################################

print('Done!')
