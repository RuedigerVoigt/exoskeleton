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
    bot_behavior={'stop_if_queue_empty': True},
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
# Check if the blocklist works before adding
# an item to the queue

logging.info('Test 1: Task 7')

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

# ################## TEST 1: TASK 8 ##################
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

# ############################################
# Test 2: Actually execute the tasks
# ############################################

# ################## TEST 2: TASK 1 ##################
# task already in the queue when its host is added to
# the blocklist. Expectation:
# - queue item is not removed
# - action will not be started in the next task as
#   the blocklist is checked

# Add another task to the queue
exo.add_save_page_code('https://www.github.com')
# NOW set the host on the blocklist
exo.block_fqdn('www.github.com')

num_expected_queue_items += 1
num_expected_labels += 0

# ################## TEST 2: TASK 2 ##################

# process the queue
exo.process_queue()

# A single item has not been processed because it
# was later added to the blocklist
num_expected_queue_items = 1
check_queue_count(1)

# ############################################
# Test 3: Delete items and see if the
# Database triggers clean up.
# ############################################

print('Done!')
