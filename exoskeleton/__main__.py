#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Exoskeleton Crawler Framework
~~~~~~~~~~~~~~~~~~~~~
A Python framework to build a basic crawler / scraper with a MariaDB backend.

Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2021 Rüdiger Voigt:
Released under the Apache License 2.0
"""

# python standard library:
from collections import Counter
# noinspection PyUnresolvedReferences
from collections import defaultdict  # noqa # pylint: disable=unused-import
import logging
from typing import Union, Optional

# Sister projects:
import compatibility
import userprovided

# import other modules of this framework
from exoskeleton import _version as version
from exoskeleton import actions
from exoskeleton import blocklist_manager
from exoskeleton import database_connection
from exoskeleton import database_schema_check
from exoskeleton import error_manager
from exoskeleton import exo_url
from exoskeleton import file_manager
from exoskeleton import job_manager
from exoskeleton import label_manager
from exoskeleton import notification_manager
from exoskeleton import queue_manager
from exoskeleton import remote_control_chrome
from exoskeleton import statistics_manager
from exoskeleton import time_manager


class Exoskeleton:
    """ Main class of the exoskeleton crawler framework. """
    # The class is complex which leads pylint to complain a lot.
    # As the complexity is needed, disable some warnings:
    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-instance-attributes
    # pylint: disable=too-many-statements
    # pylint: disable=too-many-locals
    # pylint: disable=too-many-public-methods
    # pylint: disable=too-many-branches

    def __init__(self,
                 database_settings: dict,
                 target_directory: str,
                 filename_prefix: str = '',
                 project_name: str = 'Bot',
                 bot_user_agent: str = 'Bot',
                 bot_behavior: Union[dict, None] = None,
                 mail_settings: Union[dict, None] = None,
                 mail_behavior: Union[dict, None] = None,
                 chrome_name: str = ''):
        "Set defaults, create instances, ..."

        compatibility.Check(
            package_name='exoskeleton',
            package_version=version.__version__,
            release_date=version.release_date,
            python_version_support={
                'min_version': '3.8',
                'incompatible_versions': [],
                'max_tested_version': '3.10'},
            nag_over_update={
                    'nag_days_after_release': 120,
                    'nag_in_hundred': 100},
            language_messages='en',
            system_support={
                'full': {'Linux', 'MacOS', 'Windows'}
            }
        )

        self.project: str = project_name.strip()
        self.user_agent: str = bot_user_agent.strip()

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # INIT: Database Setup / Establish a Database Connection
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        # Init database Connection
        self.db = database_connection.DatabaseConnection(database_settings)
        self.cur = self.db.get_cursor()

        self.db_check = database_schema_check.DatabaseSchemaCheck(self.db)

        self.stats = statistics_manager.StatisticsManager(self.db)

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # INIT: Mail / Notification Setup
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        mail_settings = dict() if not mail_settings else mail_settings
        mail_behavior = dict() if not mail_behavior else mail_behavior

        self.milestone: Optional[int] = mail_behavior.get('milestone_num',
                                                          None)
        if self.milestone and not isinstance(self.milestone, int):
            raise ValueError('milestone_num must be integer!')



        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # INIT: Bot Behavior
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        if bot_behavior:
            userprovided.parameters.validate_dict_keys(
                dict_to_check=bot_behavior,
                allowed_keys={'connection_timeout',
                              'queue_max_retries',
                              'queue_revisit',
                              'rate_limit_wait',
                              'stop_if_queue_empty',
                              'wait_min',
                              'wait_max'},
                necessary_keys=None,
                dict_name='bot_behavior')
        else:
            bot_behavior = dict()

        # Seconds until a connection times out:
        self.connection_timeout: int = userprovided.parameters.int_in_range(
            "self.connection_timeout",
            bot_behavior.get('connection_timeout', 60),
            1, 60, 50)

        # Init Classes

        self.time = time_manager.TimeManager(
            bot_behavior.get('wait_min', 5),
            bot_behavior.get('wait_max', 30))

        self.notify = notification_manager.NotificationManager(
            self.project, mail_settings, mail_behavior, self.time, self.stats, self.milestone)

        self.labels = label_manager.LabelManager(self.db)

        self.file = file_manager.FileManager(
            self.db,
            target_directory,
            filename_prefix)

        self.errorhandling = error_manager.CrawlingErrorManager(
            self.db,
            bot_behavior.get('queue_max_retries', 3),
            bot_behavior.get('rate_limit_wait', 1860)
            )

        self.controlled_browser = remote_control_chrome.RemoteControlChrome(
            chrome_name,
            self.errorhandling,
            self.stats)

        self.action = actions.ExoActions(
            self.db,
            self.stats,
            self.file,
            self.time,
            self.errorhandling,
            self.controlled_browser,
            self.user_agent,
            self.connection_timeout)

        self.blocklist = blocklist_manager.BlocklistManager(self.db)

        self.queue = queue_manager.QueueManager(
            self.db,
            self.blocklist,
            self.time,
            self.stats,
            self.action,
            self.notify,
            self.labels,
            bot_behavior)

        self.jobs = job_manager.JobManager(self.db)

        # Create other objects
        self.cnt: Counter = Counter()

    # #########################################################################
    # MAKE SOME METHOD CALLS EASILY ACCESSIBLE
    # #########################################################################

    def random_wait(self) -> None:
        "Wait for a random time within the limits set to init TimeManager."
        self.time.random_wait()

    def process_queue(self) -> None:
        "Process the queue"
        self.queue.process_queue()

    def return_page_code(self,
                         url: Union[exo_url.ExoUrl, str]) -> str:
        "Immediately return a page's code. Do *not* store it in the database."
        if not url:
            raise ValueError('Missing URL')
        if not isinstance(url, exo_url.ExoUrl):
            url = exo_url.ExoUrl(url)
        return self.action.return_page_code(url)

    # QUEUE MANAGEMENT:

    def add_file_download(self,
                          url: Union[exo_url.ExoUrl, str],
                          labels_master: set = None,
                          labels_version: set = None,
                          force_new_version: bool = False) -> Optional[str]:
        "Add a file download URL to the queue"
        if not isinstance(url, exo_url.ExoUrl):
            url = exo_url.ExoUrl(url)
        uuid = self.queue.add_to_queue(url, 1, labels_master,
                                       labels_version, False,
                                       force_new_version)
        return uuid

    def add_save_page_code(self,
                           url: Union[exo_url.ExoUrl, str],
                           labels_master: set = None,
                           labels_version: set = None,
                           prettify_html: bool = False,
                           force_new_version: bool = False) -> Optional[str]:
        "Add an URL to the queue to save its HTML code into the database."
        if not isinstance(url, exo_url.ExoUrl):
            url = exo_url.ExoUrl(url)
        uuid = self.queue.add_to_queue(url, 2, labels_master, labels_version,
                                       prettify_html, force_new_version)
        return uuid

    def add_page_to_pdf(self,
                        url: Union[exo_url.ExoUrl, str],
                        labels_master: set = None,
                        labels_version: set = None,
                        force_new_version: bool = False) -> Optional[str]:
        "Add an URL to the queue to print it to PDF with headless Chrome. "
        if not isinstance(url, exo_url.ExoUrl):
            url = exo_url.ExoUrl(url)
        if not self.controlled_browser.browser_present:
            logging.warning(
                'Will add this task to the queue, but without Chrome or ' +
                'Chromium it cannot run! Provide the path to the ' +
                'executable when you initialize exoskeleton.')
        uuid = self.queue.add_to_queue(url, 3, labels_master, labels_version,
                                       False, force_new_version)
        return uuid

    def add_save_page_text(self,
                           url: Union[exo_url.ExoUrl, str],
                           labels_master: set = None,
                           labels_version: set = None,
                           force_new_version: bool = False) -> Optional[str]:
        """Add the task 'Extract the text (not the code) from a HTML page and
           store it into the database' to the queue.
           This can be useful for some language processing tasks, but compared
           to add_save_page_code this removes the possiblity to work on a
           specific part using a CSS selector."""
        if not isinstance(url, exo_url.ExoUrl):
            url = exo_url.ExoUrl(url)
        uuid = self.queue.add_to_queue(url, 4, labels_master, labels_version,
                                       True, force_new_version)
        return uuid

    def delete_from_queue(self,
                          queue_id: str) -> None:
        "Remove all label links from a queue item and then delete it."
        self.queue.delete_from_queue(queue_id)
