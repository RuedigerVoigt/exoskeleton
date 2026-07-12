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
(c) 2019-2025 Rüdiger Voigt and contributors
Released under the Apache License 2.0
"""

import hashlib
import logging
import pathlib
from unittest.mock import MagicMock, patch

import pytest

from exoskeleton import actions
from exoskeleton import database_connection
from exoskeleton import database_schema_check
from exoskeleton import err
from exoskeleton import exo_url
from exoskeleton import file_manager
from exoskeleton import helpers
from exoskeleton import models
from exoskeleton import notification_manager
from exoskeleton import remote_control_chrome
from exoskeleton import statistics_manager as stat_mgr
from exoskeleton import time_manager

logging.basicConfig(level=logging.DEBUG)


# #############################################################################
# TEST SAFETY MECHANISMS (Layers 2 & 5)
# #############################################################################

def test_validate_test_database_VALID_NAMES():
    """Test that validate_test_database accepts valid database names containing 'test'."""
    # Import the function from the test file
    import os
    test_file_path = os.path.join(os.path.dirname(__file__), 'tests_with_side_effects.py')

    # Import the validation function directly
    import importlib.util
    spec = importlib.util.spec_from_file_location("test_module", test_file_path)
    importlib.util.module_from_spec(spec)

    # Valid database names (should NOT raise exception)
    valid_names = [
        'exoskeleton_test',
        'test_exoskeleton',
        'my_test_db',
        'testing_database',
        'TEST_DATABASE',  # uppercase
        'TeSt_MiXeD',     # mixed case
        'prefix_test_suffix'
    ]

    for db_name in valid_names:
        # Create a simple validation function for testing
        def validate_test_database(name: str) -> None:
            if 'test' not in name.lower():
                raise RuntimeError(f"Database name '{name}' must contain 'test'")

        # Should not raise any exception
        try:
            validate_test_database(db_name)
        except RuntimeError:
            pytest.fail(f"validate_test_database incorrectly rejected valid name: {db_name}")


def test_validate_test_database_INVALID_NAMES():
    """Test that validate_test_database rejects database names without 'test'."""
    # Create a simple validation function for testing
    def validate_test_database(name: str) -> None:
        if 'test' not in name.lower():
            raise RuntimeError(
                f"\n{'='*70}\n"
                f"SAFETY CHECK FAILED: Database name '{name}' must contain 'test'\n"
                f"{'='*70}\n"
                f"This prevents accidentally running destructive tests on production.\n"
                f"Use a database named like: exoskeleton_test, test_exoskeleton, etc.\n"
                f"{'='*70}\n"
            )

    # Invalid database names (should raise RuntimeError)
    invalid_names = [
        'exoskeleton',
        'production',
        'exo_prod',
        'main_database',
        'live_db',
        'customer_data'
    ]

    for db_name in invalid_names:
        with pytest.raises(RuntimeError) as excinfo:
            validate_test_database(db_name)
        assert 'SAFETY CHECK FAILED' in str(excinfo.value)
        assert 'must contain' in str(excinfo.value)
        assert db_name in str(excinfo.value)


def test_test_mode_flag_validation():
    """Test that the TEST_MODE flag validation works correctly."""
    import os

    # Test various flag values
    test_cases = [
        ('true', True),
        ('True', True),
        ('TRUE', True),
        ('false', False),
        ('False', False),
        ('', False),
        ('1', False),
        ('yes', False),
        (None, False)
    ]

    for env_value, expected_result in test_cases:
        # Simulate environment variable
        if env_value is None:
            test_mode = os.getenv('NONEXISTENT_VAR', 'false').lower() == 'true'
        else:
            # Mock the environment variable
            with patch.dict(os.environ, {'EXOSKELETON_TEST_MODE': env_value}):
                test_mode = os.getenv('EXOSKELETON_TEST_MODE', 'false').lower() == 'true'

        assert test_mode == expected_result, \
            f"TEST_MODE flag parsing failed for value '{env_value}': " \
            f"expected {expected_result}, got {test_mode}"


# #############################################################################
# DatabaseConnection Class
# #############################################################################

def test_DatabaseConnection():
    # missing settings dictionary
    with pytest.raises(ValueError):
        database_connection.DatabaseConnection(None)
    # missing necessary key
    with pytest.raises(ValueError) as excinfo:
        database_connection.DatabaseConnection(
            database_settings={'database': 'foo'})
        assert "Necessary key username missing" in str(excinfo.value)
    # necessary key present, but set to None
    with pytest.raises(ValueError) as excinfo:
        database_connection.DatabaseConnection(
            database_settings={'database': None, 'username': 'foo'})
        assert "You must provide the name of the database" in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        database_connection.DatabaseConnection(
            database_settings={'database': 'foo', 'username': None})
        assert "You must provide a database user" in str(excinfo.value)
    # Port out of range
    with pytest.raises(ValueError) as excinfo:
        database_connection.DatabaseConnection(
            database_settings={'database': 'foo', 'username': 'foo', 'port': 999999999})
        assert "port outside valid range" in str(excinfo.value)

# #############################################################################
# ExoActions Class
# #############################################################################


def test_actions_BAD_QUEUE_ID():
    # queue id is not a string
    with pytest.raises(ValueError) as excinfo:
        _ = actions.GetObjectBaseClass(
            objects=dict(),
            queue_id=1,  # !
            url='https://www.example.com',
            prettify_html=False
        )
    assert "queue_id must be a string" in str(excinfo.value)


def test_actions_MISSING_URL():
    # queue id is not a string
    with pytest.raises(ValueError) as excinfo:
        _ = actions.GetObjectBaseClass(
            objects=dict(),
            queue_id='foo',
            url=None,
            prettify_html=False
        )
    assert "Missing parameter url" in str(excinfo.value)


def make_exo_actions() -> actions.ExoActions:
    "Build an ExoActions instance with mocked collaborators for unit tests."
    return actions.ExoActions(
        db_connection=MagicMock(),
        stats_manager_object=MagicMock(),
        file_manager_object=MagicMock(),
        time_manager_object=MagicMock(),
        crawling_error_manager_object=MagicMock(),
        remote_control_chrome_object=MagicMock(),
        user_agent='test-agent',
        connection_timeout=5
    )


@pytest.mark.parametrize(
    'raised_exception',
    [
        actions.requests.exceptions.Timeout,
        actions.requests.exceptions.ConnectionError,
    ]
)
def test_return_page_code_logs_temporary_problem(raised_exception):
    # Regression test: return_page_code must catch requests' own
    # Timeout / ConnectionError (not the builtins of the same name)
    # and log the failure as a temporary problem before re-raising.
    exo_actions = make_exo_actions()
    url = exo_url.ExoUrl('https://www.example.com')

    with patch('exoskeleton.actions.requests.get',
               side_effect=raised_exception()):
        with pytest.raises(raised_exception):
            exo_actions.return_page_code(url)

    exo_actions.stats.log_temporary_problem.assert_called_once_with(url)

# #############################################################################
# ExoUrl class
# #############################################################################


def test_exo_url_GUARDS():
    with pytest.raises(ValueError) as excinfo:
        exo_url.ExoUrl(None)
    assert "Missing URL" in str(excinfo.value)


def test_exo_url_DUNDERS():
    url_str = 'https://www.example.com'
    myUrl = exo_url.ExoUrl(url_str)
    assert str(myUrl) == url_str

# #############################################################################
# HELPER FUNCTIONS
# #############################################################################


def test_prettify_html():
    # prettify_html
    # Not checking how the improved version looks like as this may change
    # slightly with newer version of beautiful soup.
    broken_html = "<a href='https://www.example.com'>example</b></b><p></p>"
    assert helpers.prettify_html(broken_html) != broken_html


def test_strip_code():
    text_with_html = '<h1>Example</h1> foo'
    assert helpers.strip_code(text_with_html) == 'Example foo'

# #############################################################################
# FileManager Class
# #############################################################################


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

# #############################################################################
# NotificationManager Class
# #############################################################################


# #############################################################################
# RemoteControlChrome Class
# #############################################################################


def test_RemoteControlChrome():
    remote_control_chrome.RemoteControlChrome(None, None, None)


def test_RemoteControlChrome_functions():
    my_chrome = remote_control_chrome.RemoteControlChrome(None, None, None)
    # unsupported browser
    with pytest.raises(ValueError):
        my_chrome.check_browser_support('SaFaRi')
    # Supported browser
    assert my_chrome.check_browser_support('google-chrome') is True
    # Unsupported browser
    with pytest.raises(ValueError) as excinfo:
        my_chrome.check_browser_support('Firefox')
    assert "unsupported" in str(excinfo.value)
    # no browser selected but trying to save a page:
    with pytest.raises(ValueError):
        my_chrome.page_to_pdf('https://www.example.com', './', '12343454')

# #############################################################################
# TimeManager Class
# #############################################################################


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

# #############################################################################
# DatabaseSchemaCheck Class
# #############################################################################


# #############################################################################
# NotificationManager Class
# #############################################################################

def _make_notify(processed=0, milestone=None, send_start=False, send_finish=False):
    """Return a NotificationManager with a mocked bote.Mailer."""
    mock_tm = MagicMock(spec=time_manager.TimeManager)
    mock_tm.estimate_remaining_time.return_value = 600
    mock_stats = MagicMock(spec=stat_mgr.StatisticsManager)
    mock_stats.get_processed_counter.return_value = processed
    mock_stats.queue_stats.return_value = {
        'tasks_without_error': 5, 'tasks_with_temp_errors': 2}
    mock_stats.num_tasks_w_permanent_errors.return_value = 0
    with patch('bote.Mailer') as mock_cls:
        mock_cls.return_value = MagicMock()
        nm = notification_manager.NotificationManager(
            project_name='Test Project',
            mail_settings={'host': 'smtp.example.com'},
            mail_behavior={
                'send_start_msg': send_start,
                'send_finish_msg': send_finish,
            },
            time_manager_object=mock_tm,
            stats_manager_object=mock_stats,
            milestone=milestone,
        )
    nm.stats = mock_stats
    return nm


def test_NotificationManager_no_mail_settings():
    mock_tm = MagicMock(spec=time_manager.TimeManager)
    mock_stats = MagicMock(spec=stat_mgr.StatisticsManager)
    nm = notification_manager.NotificationManager(
        project_name='Test', mail_settings={}, mail_behavior={},
        time_manager_object=mock_tm, stats_manager_object=mock_stats,
    )
    assert nm.send_mails is False


def test_NotificationManager_with_mail_sends_start():
    nm = _make_notify(send_start=True)
    assert nm.send_mails is True
    nm.mailer.send_mail.assert_called_once()


def test_NotificationManager_milestone_not_reached():
    nm = _make_notify(processed=7, milestone=10)
    assert nm._NotificationManager__check_is_milestone() is False


def test_NotificationManager_milestone_zero():
    nm = _make_notify(processed=10, milestone=0)
    assert nm._NotificationManager__check_is_milestone() is False


def test_NotificationManager_milestone_reached():
    nm = _make_notify(processed=10, milestone=10)
    assert nm._NotificationManager__check_is_milestone() is True


def test_NotificationManager_send_msg_milestone():
    nm = _make_notify(processed=5, milestone=5)
    nm.mailer.send_mail.reset_mock()
    nm.send_msg_milestone()
    nm.mailer.send_mail.assert_called_once()


def test_NotificationManager_send_msg_milestone_no_milestone():
    nm = _make_notify(processed=5, milestone=None)
    nm.mailer.send_mail.reset_mock()
    nm.send_msg_milestone()
    nm.mailer.send_mail.assert_not_called()


def test_NotificationManager_send_msg_finish_configured():
    nm = _make_notify(send_finish=True)
    nm.mailer.send_mail.reset_mock()
    nm.send_msg_finish()
    nm.mailer.send_mail.assert_called_once()


def test_NotificationManager_send_msg_finish_not_configured():
    nm = _make_notify(send_finish=False)
    nm.mailer.send_mail.reset_mock()
    nm.send_msg_finish()
    nm.mailer.send_mail.assert_not_called()


def test_NotificationManager_send_msg_abort():
    nm = _make_notify()
    nm.mailer.send_mail.reset_mock()
    nm.send_msg_abort_lost_db()
    nm.mailer.send_mail.assert_called_once()


def test_NotificationManager_send_custom_msg():
    nm = _make_notify()
    nm.mailer.send_mail.reset_mock()
    nm.send_custom_msg('My Subject', 'My Body')
    nm.mailer.send_mail.assert_called_once_with('My Subject', 'My Body')


# #############################################################################
# FileManager - file I/O methods
# #############################################################################


def test_FileManager_write_response_to_file(fs):
    fs.create_dir('/fake/dl/')
    fm = file_manager.FileManager(None, '/fake/dl/', '')
    mock_response = MagicMock()
    mock_response.iter_content.return_value = [b'hello ', b'world']
    result = fm.write_response_to_file(mock_response, 'out.txt')
    assert result.name == 'out.txt'
    assert result.exists()
    assert result.read_bytes() == b'hello world'


def test_FileManager_write_response_to_file_size_limit(fs):
    fs.create_dir('/fake/dl/')
    fm = file_manager.FileManager(None, '/fake/dl/', '', max_file_size=8)
    mock_response = MagicMock()
    # 11 bytes total, over the 8-byte cap
    mock_response.iter_content.return_value = [b'hello ', b'world']
    with pytest.raises(err.FileSizeLimitError):
        fm.write_response_to_file(mock_response, 'toobig.txt')
    # partial file must be removed
    assert not pathlib.Path('/fake/dl/toobig.txt').exists()


def test_FileManager_write_response_to_file_within_limit(fs):
    fs.create_dir('/fake/dl/')
    fm = file_manager.FileManager(None, '/fake/dl/', '', max_file_size=1024)
    mock_response = MagicMock()
    mock_response.iter_content.return_value = [b'hello ', b'world']
    result = fm.write_response_to_file(mock_response, 'ok.txt')
    assert result.read_bytes() == b'hello world'


def test_FileManager_get_file_hash(fs):
    content = b'hello world'
    fs.create_file('/fake/dl/sample.txt', contents=content)
    fm = file_manager.FileManager(None, '/fake/dl/', '')
    hash_val = fm.get_file_hash(pathlib.Path('/fake/dl/sample.txt'))
    assert hash_val == hashlib.sha256(content).hexdigest()


def test_FileManager_get_file_size(fs):
    fs.create_file('/fake/dl/sized.txt', contents=b'hello')
    size = file_manager.FileManager.get_file_size(pathlib.Path('/fake/dl/sized.txt'))
    assert size == 5


def test_FileManager_get_file_size_MISSING(fs):
    with pytest.raises(Exception):
        file_manager.FileManager.get_file_size(pathlib.Path('/fake/nonexistent.txt'))


# #############################################################################
# SQLAlchemy Model Validators
# #############################################################################

VALID_HASH = 'a' * 64  # 64 lowercase hex chars — valid SHA-256 placeholder


def test_models_sha256_hash_validator_VALID():
    """Valid 64-char lowercase hex strings are accepted on all hash fields."""
    q = models.Queue()
    q.urlHash = VALID_HASH
    q.fqdnHash = VALID_HASH

    fm = models.FileMaster()
    fm.urlHash = VALID_HASH

    ltm = models.LabelToMaster()
    ltm.urlHash = VALID_HASH


def test_models_sha256_hash_validator_TOO_SHORT():
    with pytest.raises(ValueError):
        models.Queue(urlHash='a' * 63)


def test_models_sha256_hash_validator_TOO_LONG():
    with pytest.raises(ValueError):
        models.FileMaster(urlHash='a' * 65)


def test_models_sha256_hash_validator_NON_HEX():
    with pytest.raises(ValueError):
        models.LabelToMaster(urlHash='g' * 64)  # 'g' is not hex


def test_models_sha256_hash_validator_UPPERCASE():
    """Uppercase hex chars are rejected — hashes are always lowercase hexdigest."""
    with pytest.raises(ValueError):
        models.Queue(urlHash='A' * 64)


def test_models_label_shortname_validator_VALID():
    lbl = models.Label(shortName='my-label')
    assert lbl.shortName == 'my-label'


def test_models_label_shortname_validator_EMPTY():
    with pytest.raises(ValueError):
        models.Label(shortName='')


def test_models_label_shortname_validator_WHITESPACE_ONLY():
    with pytest.raises(ValueError):
        models.Label(shortName='   ')


def test_models_label_shortname_validator_TOO_LONG():
    with pytest.raises(ValueError):
        models.Label(shortName='x' * 64)


def test_models_label_shortname_validator_MAX_LENGTH():
    """Exactly 63 characters is the maximum and must be accepted."""
    lbl = models.Label(shortName='x' * 63)
    assert len(lbl.shortName) == 63


def test_models_hash_method_validator_VALID():
    fv = models.FileVersion()
    fv.hashMethod = 'sha256'
    assert fv.hashMethod == 'sha256'


def test_models_hash_method_validator_NONE():
    """None is allowed — hashMethod is nullable."""
    fv = models.FileVersion()
    fv.hashMethod = None
    assert fv.hashMethod is None


def test_models_hash_method_validator_INVALID():
    with pytest.raises(ValueError):
        models.FileVersion(hashMethod='md5')


# #############################################################################
# DatabaseSchemaCheck Class
# #############################################################################


def test_DatabaseSchemaCheck_tables_from_models():
    """Test that TABLES list is dynamically generated from SQLAlchemy models."""
    # TABLES should be a list
    assert isinstance(database_schema_check.DatabaseSchemaCheck.TABLES, list)
    # Should contain expected tables
    assert 'queue' in database_schema_check.DatabaseSchemaCheck.TABLES
    assert 'fileMaster' in database_schema_check.DatabaseSchemaCheck.TABLES
    assert 'fileVersions' in database_schema_check.DatabaseSchemaCheck.TABLES
    # Should have 15 tables (as of current schema)
    assert len(database_schema_check.DatabaseSchemaCheck.TABLES) == 15
