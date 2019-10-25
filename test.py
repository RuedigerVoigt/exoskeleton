#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

import logging

import exoskeleton

import credentials

logging.basicConfig(level=logging.DEBUG)

queueManager = exoskeleton.Exoskeleton(
    database_host='ruediger-voigt.eu',
    database_name='exoskeleton',
    database_user=credentials.user,
    database_passphrase=credentials.passphrase,
    mail_admin='ruedigervoigt@gmx.net',
    queue_stop_on_empty=True # NEW
)

print(queueManager.num_items_in_queue())
print(queueManager.estimate_remaining_time())


queueManager.add_file_download('https://www.ruediger-voigt.eu/examplefile.txt')
queueManager.add_file_download('https://www.ruediger-voigt.eu/file_does_not_exist.pdf')
queueManager.add_save_page_code('https://www.ruediger-voigt.eu/')

print(queueManager.num_items_in_queue())

queueManager.process_queue()

Einstellmöglichkeit, dass das keine Files überschrieben soll!