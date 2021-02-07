#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
The class CrawlingErrorManager manages errors that occur while
crawling. It marks permanent errors and adds delays.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
Released under the Apache License 2.0
"""
# standard library:
import logging
from typing import Optional

# external dependencies:
import userprovided
import pymysql

from exoskeleton import database_connection


class CrawlingErrorManager:
    """Manage errors that occur while crawling."""

    # If there is a temporary error, exoskeleton delays the
    # next try until the configured maximum of tries is
    # reached.
    # The time between tries is definied here to be able
    # to overwrite it in case of an automatic tests to
    # avoid multi-hour runtimes.
    # Steps: 1/4h, 1/2h, 1h, 3h, 6h
    DELAY_TRIES = (900, 1800, 3600, 10800, 21600)

    def __init__(self,
                 db_connection: database_connection.DatabaseConnection,
                 queue_max_retries: int,
                 rate_limit_wait_seconds: int) -> None:
        self.cur: pymysql.cursors.Cursor = db_connection.get_cursor()
        # Maximum number of retries if downloading a page/file failed:
        self.queue_max_retries: int = queue_max_retries
        self.queue_max_retries = userprovided.parameters.int_in_range(
            "queue_max_retries", self.queue_max_retries, 0, 10, 3)

        # Seconds to wait before contacting the same FQDN again, after the bot
        # hit a rate limit. Defaults to 1860 seconds (i.e. 31 minutes):
        self.rate_limit_wait: int = rate_limit_wait_seconds

    def add_crawl_delay(self,
                        queue_id: str,
                        error_type: Optional[int] = None) -> None:
        """In case of a timeout or a temporary error increment the counter for
        the number of tries by one. If the configured maximum of tries
        was reached, mark it as a permanent error. Otherwise add a delay,
        so exoskelton does not try the same task again. As multiple tasks
        may affect the same URL, the delay is added to all of them."""
        wait_time = 0

        # Increase the tries counter
        self.cur.execute('UPDATE queue ' +
                         'SET numTries = numTries + 1 ' +
                         'WHERE id =%s;', (queue_id, ))
        # How many times this task was tried?
        self.cur.execute('SELECT numTries FROM queue WHERE id = %s;',
                         (queue_id, ))
        response = self.cur.fetchone()
        num_tries = int(response[0]) if response else 0  # type: ignore[index]

        # Does the number of tries exceed the configured maximum?
        if num_tries == self.queue_max_retries:
            # This is treated as a *permanent* failure!
            logging.error('Giving up: too many tries for queue item %s',
                          queue_id)
            self.mark_permanent_error(queue_id, 3)
        else:
            logging.info('Adding crawl delay to queue item %s', queue_id)
            # Using the class constant DELAY_TRIES because it can be easily
            # overwritten for automatic testing!
            if num_tries == 1:
                wait_time = self.DELAY_TRIES[0]  # 15 minutes
            elif num_tries == 2:
                wait_time = self.DELAY_TRIES[1]  # 30 minutes
            elif num_tries == 3:
                wait_time = self.DELAY_TRIES[2]  # 1 hour
            elif num_tries == 4:
                wait_time = self.DELAY_TRIES[3]  # 3 hours
            elif num_tries > 4:
                wait_time = self.DELAY_TRIES[4]  # 6 hours

            # Add the same delay to all tasks accesing the same URL MariaDB /
            # MySQL throws an error if the same table is specified both
            # as a target for 'UPDATE' and a source for data.
            # Therefore, two steps instead of a Sub-Select.
            self.cur.execute('SELECT urlHash FROM queue WHERE id = %s;',
                             (queue_id, ))
            response = self.cur.fetchone()
            url_hash = response[0] if response else None  # type: ignore[index]
            if not url_hash:
                raise ValueError('Missing urlHash. Cannot add crawl delay.')

            self.cur.execute('UPDATE queue ' +
                             'SET delayUntil = ADDTIME(NOW(), %s) ' +
                             'WHERE urlHash = %s;',
                             (wait_time, url_hash))
            # Add the error type to the specific task that caused the delay
            if error_type:
                self.cur.execute('UPDATE queue ' +
                                 'SET causesError = %s ' +
                                 'WHERE id = %s;',
                                 (error_type, queue_id))

    def mark_permanent_error(self,
                             queue_id: str,
                             error: int) -> None:
        """ Mark item in queue that causes a permanent error.
            Without this exoskeleton would try to execute the
            task over and over again."""

        self.cur.execute('UPDATE queue ' +
                         'SET causesError = %s, ' +
                         'delayUntil = NULL ' +
                         'WHERE id = %s;', (error, queue_id))
        logging.info('Marked queue-item %s as causing a permanent problem.',
                     queue_id)

    def forget_specific_error(self,
                              specific_error: int) -> None:
        """Treat all queued tasks, that are marked to cause a *specific*
        error, as if they are new tasks by removing that mark and any delay.
        The number of the error has to correspond to the errorType
        database table."""
        self.cur.execute("UPDATE queue SET " +
                         "causesError = NULL, " +
                         "numTries = 0,"
                         "delayUntil = NULL " +
                         "WHERE causesError = %s;",
                         (specific_error, ))

    def forget_error_group(self,
                           permanent: bool) -> None:
        """Forget all errors that are permanent if that parameter is True,
           else forget all that are NOT permanent."""
        self.cur.execute("UPDATE queue SET " +
                         "causesError = NULL, " +
                         "numTries = 0, " +
                         "delayUntil = NULL " +
                         "WHERE causesError IN (" +
                         "    SELECT id from errorType WHERE permanent = %s);",
                         (1 if permanent else 0, ))

    def forget_all_errors(self) -> None:
        """Treat all queued tasks, that are marked to cause any type of
        error, as if they are new tasks by removing that mark and any delay."""
        self.cur.execute("UPDATE queue SET " +
                         "causesError = NULL, " +
                         "numTries = 0, " +
                         "delayUntil = NULL;")

    def add_rate_limit(self,
                       fqdn: str) -> None:
        """If a bot receives the statuscode 429 ('too many requests') it hit
           a rate limit. Adding the fully qualified domain name to the rate
           limit list, ensures that this FQDN is not contacted for a
           predefined time."""
        msg = (f"Bot hit a rate limit with {fqdn}. Will not try to " +
               f"contact this host for {self.rate_limit_wait} seconds.")
        logging.error(msg)
        # Always use SEC_TO_TIME() to avoid unexpected behavior if
        # just adding seconds as a plain number.
        self.cur.execute('INSERT INTO rateLimits ' +
                         '(fqdnHash, fqdn, noContactUntil) VALUES ' +
                         '(SHA2(%s,256), %s, ADDTIME(NOW(), SEC_TO_TIME(%s))) ' +
                         'ON DUPLICATE KEY UPDATE ' +
                         'noContactUntil = ADDTIME(NOW(), SEC_TO_TIME(%s));',
                         (fqdn, fqdn, self.rate_limit_wait,
                          self.rate_limit_wait))

    def forget_specific_rate_limit(self,
                                   fqdn: str) -> None:
        """Forget that the bot hit a rate limit for a specific FQDN."""
        self.cur.execute('DELETE FROM rateLimits ' +
                         'WHERE fqdnHash = SHA2(%s,256);',
                         (fqdn, ))

    def forget_all_rate_limits(self) -> None:
        """Forget all rate limits the bot hit."""
        self.cur.execute('TRUNCATE TABLE rateLimits;')
