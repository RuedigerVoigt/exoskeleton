#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Automatic Tests for exoskeleton

! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! !  ! ! ! ! ! ! ! ! !
There are two groups of automatic tests for exoskeleton:
* Unit-tests without side-effects that run extremly fast and cover about 1/3.
* A system-test that actually interacts with a database and the network.

This file contains the fast tests without side effects. These tests can be run
with Linux, MacOS and Windows while the other tests require Linux as the
database service is not yet available for the other systems. See:
https://docs.github.com/en/actions/guides/about-service-containers
! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! !  ! ! ! ! ! ! ! ! !


While the latter one is much slower, it also tests the SQL code and the code
interacting with the network. It simulates a user interacting with the
framework.
Even though much *could* be mocked, it is better to actually run it:
There might be changes to the SQL part and sometimes the DBMS itself
might introduce a bug.

To run only these tests here:
coverage run --source exoskeleton -m pytest tests_without_side_effects.py
To generate a report limited to that run afterwards:
coverage html
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2021 Rüdiger Voigt
Released under the Apache License 2.0
"""

import logging
from unittest.mock import patch

logging.basicConfig(level=logging.DEBUG)

import pyfakefs
import pytest


from exoskeleton import actions
from exoskeleton import database_connection
from exoskeleton import file_manager
from exoskeleton import notification_manager
from exoskeleton import remote_control_chrome
from exoskeleton import time_manager

# #############################################################################
# TESTS WITHOUT A FULL INSTANCE
# #############################################################################


def test_DatabaseConnection():
    # missing settings dictionary
    with pytest.raises(ValueError):
        database_connection.DatabaseConnection(None)
    # missing necessary key
    with pytest.raises(ValueError) as excinfo:
        database_connection.DatabaseConnection(database_settings={'database': 'foo'})
        assert "Necessary key username missing" in str(excinfo.value)
    # necessary key present, but set to None
    with pytest.raises(ValueError) as excinfo:
        database_connection.DatabaseConnection(database_settings={'database': None, 'username': 'foo'})
        assert "You must provide the name of the database" in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        database_connection.DatabaseConnection(database_settings={'database': 'foo', 'username': None})
        assert "You must provide a database user" in str(excinfo.value)
    # Port out of range
    with pytest.raises(ValueError) as excinfo:
        database_connection.DatabaseConnection(database_settings={'database': 'foo', 'username': 'foo', 'port': 999999999})
        assert "port outside valid range" in str(excinfo.value)


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


# fs is a fixture provided by pyfakefs
def test_FileManager_target_directory(fs):
    fs.create_file('/fake/example.file')
    # directory exists
    assert file_manager.FileManager._FileManager__check_target_directory("/fake/")
    # directory is not exist
    with pytest.raises(FileNotFoundError):
        assert file_manager.FileManager._FileManager__check_target_directory("/fake/nonexistent")
    # user supplied file instead of directory
    with pytest.raises(AttributeError):
        assert file_manager.FileManager._FileManager__check_target_directory("/fake/example.file")
    # Missing parameter
    with pytest.raises(TypeError):
        assert file_manager.FileManager._FileManager__check_target_directory()
    # Parameter, but no directory provided. Fallback to cwd
    assert file_manager.FileManager._FileManager__check_target_directory(None)
    assert file_manager.FileManager._FileManager__check_target_directory(' ')


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
    # at wait tresholds:
    my_tm.wait_min = 30
    my_tm.wait_max = 50
    my_tm.increase_wait()
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
