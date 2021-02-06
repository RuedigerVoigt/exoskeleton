#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Automatic Tests for salted

! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! !
IMPORTANT:
For the moment the relevant test is in the file system-test.py.
That is a complete run with different scenarios which test the interaction
between this application, the database and the network.
That is run automatically.
These test here cover only around 1/3 of the code.
! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! !


To run these tests here:
coverage run --source exoskeleton -m pytest tests.py
To generate a report afterwards.
coverage html
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2021 Rüdiger Voigt
Released under the Apache License 2.0
"""

from unittest.mock import patch

import pytest

from exoskeleton import actions
from exoskeleton import database_connection
from exoskeleton import file_manager
from exoskeleton import notification_manager
from exoskeleton import remote_control_chrome
from exoskeleton import statistics_manager
from exoskeleton import time_manager


def test_DatabaseConnection():
    # missing settings dictionary
    with pytest.raises(ValueError):
        database_connection.DatabaseConnection(None)


def test_FileManager_functions():
    # empty string
    assert file_manager.FileManager._FileManager__clean_prefix('') == ''
    # only whitespace
    assert file_manager.FileManager._FileManager__clean_prefix('   ') == ''
    # exactly 16 characters ( = allowed length) plus whitespace
    assert file_manager.FileManager._FileManager__clean_prefix('   1234567890123456  ') == '1234567890123456'
    # more than 16 characters ( = allowed length)
    with pytest.raises(ValueError):
        assert file_manager.FileManager._FileManager__clean_prefix('12345678901234567') == ''
    assert file_manager.FileManager._FileManager__clean_prefix(None) == ''


def test_Notificationmanager():
    notification_manager.NotificationManager('test', None, None)


def test_RemoteControlChrome():
    remote_control_chrome.RemoteControlChrome(None, None, None)


def test_RemoteControlChrome_functions():
    my_chrome = remote_control_chrome.RemoteControlChrome(None, None, None)
    with pytest.raises(ValueError):
        assert my_chrome.check_browser_support('test') is False
    with pytest.raises(ValueError):
        assert my_chrome.check_browser_support('SaFaRi') is False
    assert my_chrome.check_browser_support('google-chrome') is True
    # no browser selected but trying to save a page:
    with pytest.raises(ValueError):
        my_chrome.page_to_pdf('https://www.example.com', './', '12343454')


def test_StatisticsManager():
    my_stats = statistics_manager.StatisticsManager(None)
    assert my_stats.cnt['processed'] == 0
    my_stats.increment_processed_counter()
    assert my_stats.cnt['processed'] == 1
    assert my_stats.get_processed_counter() == 1


def test_StatisticsManager_functions():
    # TO DO
    #my_stats.update_host_statistics('https://www.example.com', 1, 0, 0, 0)
    pass


def test_TimeManager():
    # No parameters
    time_manager.TimeManager()
    # Valid parameters
    time_manager.TimeManager(10, 40)
    # non numeric value for wait_min
    with pytest.raises(ValueError):
        time_manager.TimeManager('abc', 40)
    # non numeric value for wait_max
    with pytest.raises(ValueError):
        time_manager.TimeManager(10, 'abc')
    # Contradiction: wait_min > wait_max
    with pytest.raises(ValueError):
        time_manager.TimeManager(100, 10)


def test_TimeManager_functions():
    my_tm = time_manager.TimeManager()
    # Increase the wait time
    min_before = my_tm.wait_min
    max_before = my_tm.wait_max
    my_tm.increase_wait()
    assert my_tm.wait_min == min_before + 1
    assert my_tm.wait_max == max_before + 1
    # process time
    assert isinstance(my_tm.get_process_time(), float) is True
    # absolute run time
    assert isinstance(my_tm.absolute_run_time(), float) is True
    # estimate for remaining time
    assert my_tm.estimate_remaining_time(0, 0) == -1  # nothing done
    assert my_tm.estimate_remaining_time(0, 10000) == -1  # nothing done
    assert my_tm.estimate_remaining_time(10000, 0) == 0
    assert isinstance(my_tm.estimate_remaining_time(10000, 10), int) is True
    # random-wait needs to be patched
    with patch('time.sleep', return_value=None):
        my_tm.random_wait()


def test_actions():
    # prettify_html
    # Not checking how the improved version looks like as this may change
    # slightly with newer version of beautiful soup.
    broken_html = "<a href='https://www.example.com'>example</b></b><p></p>"
    assert actions.ExoActions.prettify_html(broken_html) != broken_html
