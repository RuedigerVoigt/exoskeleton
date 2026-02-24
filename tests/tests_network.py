#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Network integration tests for exoskeleton.

These tests make real HTTP requests to a controlled server (ruediger-voigt.eu)
and therefore require network access and are slower than the other test files.
They are kept in a separate file so that CI failures are immediately
recognisable as network-related rather than logic or database failures.

Run order within this file matters:
  test_exceed_retries must run before test_forget_errors because the latter
  inspects the error state left by the former.

~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2025 Rüdiger Voigt
Released under the Apache License 2.0
"""

import logging
import os

import pytest
from dotenv import load_dotenv

import exoskeleton
from exoskeleton import exo_url
from exoskeleton import models

logging.basicConfig(level=logging.DEBUG)

# Load test configuration from .env.test (same convention as the other test files).
# In CI/CD the file does not exist and defaults are used instead.
if os.path.exists('.env.test'):
    load_dotenv('.env.test')

database_settings = {
    'host': os.getenv('TEST_DB_HOST', 'localhost'),
    'port': int(os.getenv('TEST_DB_PORT', '3306')),
    'database': os.getenv('TEST_DB_NAME', 'exoskeleton_test'),
    'username': os.getenv('TEST_DB_USER', 'exoskeleton_test'),
    'passphrase': os.getenv('TEST_DB_PASSWORD', 'exoskeleton_test'),
}

exo = exoskeleton.Exoskeleton(
    project_name='Exoskeleton Network Test',
    database_settings=database_settings,
    bot_behavior={
        'queue_max_retries': 6,
        'wait_min': 1,
        'wait_max': 5,
        'connection_timeout': 30,
        'queue_revisit': 5,
        'stop_if_queue_empty': True,
    },
    filename_prefix='EXO_',
    target_directory='./fileDownloads',
)

# Shorten retry delays so test_exceed_retries finishes in reasonable time.
exo.errorhandling.DELAY_TRIES = (1, 1, 1, 1, 1)


# #############################################################################
# HELPERS
# #############################################################################

def check_error_codes(expectation: set) -> None:
    "Assert that the set of error codes in the queue matches *expectation*."
    session = exo.db.get_session()
    rows = session.query(models.Queue.causesError).filter(
        models.Queue.causesError.isnot(None)
    ).order_by(models.Queue.causesError).all()
    actual = {row[0] for row in rows}
    if actual != expectation:
        raise AssertionError(
            f"Wrong error codes in queue: {actual} (expected {expectation})")


# #############################################################################
# TESTS
# #############################################################################

def test_return_page_code():
    exo.return_page_code('https://www.ruediger-voigt.eu/')
    exo.return_page_code(exo_url.ExoUrl('https://www.ruediger-voigt.eu/'))
    with pytest.raises(ValueError) as excinfo:
        exo.return_page_code(None)
    assert 'Missing URL' in str(excinfo.value)
    with pytest.raises(RuntimeError) as excinfo:
        exo.return_page_code("https://www.ruediger-voigt.eu/throw-402.html")
    assert 'Cannot return page code' in str(excinfo.value)


@pytest.mark.timeout(300)
def test_exceed_retries():
    """The controlled server always returns the HTTP status code named in the
    URL path.  Verify that:
    - temporary errors (500) are retried up to queue_max_retries times and
      then marked as 'gave_up' (error code 3)
    - permanent errors (402, 404, 407, 410, 451) are flagged immediately
    """
    uuid_code_500 = exo.add_save_page_code(
        "https://www.ruediger-voigt.eu/throw-500.html")
    exo.add_save_page_code("https://www.ruediger-voigt.eu/throw-402.html")
    exo.add_save_page_code("https://www.ruediger-voigt.eu/throw-404.html")
    exo.add_save_page_code("https://www.ruediger-voigt.eu/throw-407.html")
    exo.add_save_page_code("https://www.ruediger-voigt.eu/throw-410.html")
    exo.add_save_page_code("https://www.ruediger-voigt.eu/throw-451.html")
    exo.process_queue()
    session = exo.db.get_session()
    queue_item = session.query(
        models.Queue.causesError, models.Queue.numTries
    ).filter(models.Queue.id == uuid_code_500).first()
    assert queue_item == (3, 6), f"Wrong result for exceeded retries: {queue_item}"


@pytest.mark.timeout(300)
def test_forget_errors():
    """Must run after test_exceed_retries.  Verify that the forget_* methods
    clear error flags correctly and that items are retried afterwards."""
    check_error_codes({3, 451, 402, 404, 407, 410})
    exo.errorhandling.forget_specific_error(404)
    check_error_codes({3, 451, 402, 407, 410})
    exo.errorhandling.forget_permanent_errors()
    check_error_codes(set())
    exo.errorhandling.forget_temporary_errors()
    # Re-process: all items should fail with the same codes again
    exo.process_queue()
    check_error_codes({3, 451, 402, 404, 407, 410})
    exo.errorhandling.forget_all_errors()
    check_error_codes(set())
    # Leave the queue empty for the next test (DELETE avoids DDL metadata invalidation)
    session = exo.db.get_session()
    session.query(models.Queue).delete(synchronize_session=False)
    session.commit()


@pytest.mark.timeout(120)
def test_handle_redirects():
    "Verify that 301 and 302 redirects are followed and final content is stored."
    redirect301 = exo.add_save_page_code(
        "https://www.ruediger-voigt.eu/redirect-301.html")
    redirect302 = exo.add_save_page_code(
        "https://www.ruediger-voigt.eu/redirect-302.html")
    exo.process_queue()

    session = exo.db.get_session()
    content_301 = session.query(models.FileContent).filter(
        models.FileContent.versionID == redirect301
    ).first()
    assert content_301 is not None
    assert content_301.pageContent == 'testfile1', 'Redirect 301 did not work.'

    content_302 = session.query(models.FileContent).filter(
        models.FileContent.versionID == redirect302
    ).first()
    assert content_302 is not None
    assert content_302.pageContent == 'testfile2', 'Redirect 302 did not work.'
