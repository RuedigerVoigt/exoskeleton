#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
JobManager for the exoskeleton framework.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2021 Rüdiger Voigt:
Released under the Apache License 2.0
"""
# standard library:
import logging
from typing import Union

# external dependencies:
import userprovided
import pymysql

from exoskeleton import database_connection
from exoskeleton import exo_url


class JobManager:
    """Jobs are used to crawl multi-page results like search engine queries.
       You provide a start URL and update it while looping through pagination.
       By doing this you can restart a paused job without starting all over."""
    # pylint: disable=raise-missing-from

    def __init__(self,
                 db_connection: database_connection.DatabaseConnection
                 ) -> None:
        "Sets defaults"
        self.cur: pymysql.cursors.Cursor = db_connection.get_cursor()

    def define_new(self,
                   job_name: str,
                   start_url: Union[exo_url.ExoUrl, str]) -> None:
        "Create a new crawl job identified by its name and add a start URL."
        if not job_name:
            raise ValueError('Provide a valid job_name')
        if not userprovided.parameters.string_in_range(job_name, 1, 127, True):
            raise ValueError('job name must be between 1 and 127 characters.')
        if not start_url:
            raise ValueError('A job needs a Start URL.')
        if not isinstance(start_url, exo_url.ExoUrl):
            start_url = exo_url.ExoUrl(start_url)
        job_name = job_name.strip()
        try:
            self.cur.callproc('define_new_job_SP', (job_name, start_url))
            logging.debug('Defined new job.')
        except pymysql.IntegrityError:
            # A job with this name already exists
            # Check if startURL is the same:
            self.cur.execute('SELECT startURL FROM jobs WHERE jobName = %s;',
                             (job_name, ))
            response = self.cur.fetchone()
            existing_start_url = response[0] if response else None  # type: ignore[index]
            if existing_start_url != start_url:
                raise ValueError('A job with the identical name but ' +
                                 '*different* startURL is already defined!')
            logging.warning(
                'A job with identical name and startURL is already defined.')

    def update_current_url(self,
                           job_name: str,
                           current_url: Union[exo_url.ExoUrl, str]) -> None:
        "Set the currentUrl for a specific job. "
        if not job_name:
            raise ValueError('Provide the job name.')
        if not current_url:
            raise ValueError('Current URL must not be empty.')
        if not isinstance(current_url, exo_url.ExoUrl):
            current_url = exo_url.ExoUrl(current_url)
        # execute returns affected rows, callproc does not
        affected_rows = self.cur.execute(
            'CALL job_update_current_url_SP(%s, %s);',
            (job_name, current_url))
        if affected_rows == 0:
            raise ValueError('A job with this name is not known.')

    def get_current_url(self,
                        job_name: str) -> str:
        """ Returns the current URL for this job. If none is stored, this
            returns the start URL.
            Raises ValueError if the job is unknown.
            Raises RuntimeError if the job is already finished."""

        self.cur.callproc('job_get_current_url_SP', (job_name, ))
        job_state = self.cur.fetchone()

        if job_state is None:
            raise ValueError('Job is unknown!')
        if job_state[0] is not None:  # type: ignore[index]
            # Field 0 contains the status: finished (not None) or not (None)
            raise RuntimeError(f"Job {job_name} already finished.")
        # Field 1 contains either the current or the start URL
        return job_state[1]  # type: ignore[index]

    def mark_as_finished(self,
                         job_name: str) -> None:
        "Mark a crawl job as finished."
        if not job_name:
            raise ValueError('Missing job_name')
        job_name = job_name.strip()
        # Contrary to callproc, execute returns the number of affected rows:
        affected_rows = self.cur.execute('CALL job_mark_as_finished_SP(%s);',
                                         (job_name, ))
        if affected_rows == 0:
            raise ValueError('A job with this name is not known.')
        logging.debug('Marked job %s as finished.', job_name)
