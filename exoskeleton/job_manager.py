#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
JobManager
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
Released under the Apache License 2.0
"""
# standard library:
import logging

# external dependencies:
import userprovided
import pymysql


class JobManager:
    """Jobs are used to crawl multi-page results like search engine queries.
       You provide a start URL and update it while looping through pagination.
       By doing this you can restart a paused job without starting all over."""
    # pylint: disable=raise-missing-from

    def __init__(self,
                 db_cursor: pymysql.cursors.Cursor) -> None:
        """Sets defaults"""

        self.cur = db_cursor

    def define_new(self,
                   job_name: str,
                   start_url: str) -> None:
        """ Create a new crawl job identified by it name and an url
            to start crawling. """
        if not job_name:
            raise ValueError('Provide a valid job_name')
        if not start_url:
            raise ValueError('A job needs a Start URL.')

        job_name = job_name.strip()

        if not userprovided.parameters.string_in_range(job_name, 1, 127):
            raise ValueError('job name must be between 1 and 127 characters.')

        try:
            self.cur.execute('INSERT INTO jobs ' +
                             '(jobName, startUrl, startUrlHash) ' +
                             'VALUES (%s, %s, SHA2(%s,256));',
                             (job_name, start_url, start_url))
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
            logging.warning('A job with identical name and startURL ' +
                            'is already defined.')

    def update_current_url(self,
                           job_name: str,
                           current_url: str) -> None:
        "Set the currentUrl for a specific job. "
        if not job_name:
            raise ValueError('Provide the job name.')
        if not current_url:
            raise ValueError('Current URL must not be empty.')

        affected_rows = self.cur.execute('UPDATE jobs ' +
                                         'SET currentURL = %s ' +
                                         'WHERE jobName = %s;',
                                         (current_url, job_name))
        if affected_rows == 0:
            raise ValueError('A job with this name is not known.')

    def get_current_url(self,
                        job_name: str) -> str:
        """ Returns the current URl for this job. If none is stored, this
        returns the start URL. Raises exception if the job is already
        finished."""

        self.cur.execute('SELECT finished FROM jobs ' +
                         'WHERE jobName = %s;',
                         (job_name, ))
        job_state = self.cur.fetchone()
        # If the job does not exist at all, then MariaDB returns None.
        # If the job exists, but the finished field has a value of NULL,
        # then MariaDB returns (None,)
        try:
            job_state = job_state[0]  # type: ignore[index]
        except TypeError:
            # Occurs if the the result was None, i.e. the job
            # does not exist.
            raise ValueError('Job is unknown!')

        if job_state is not None:
            # i.e. the finished field is not empty
            raise RuntimeError(f"Job already finished at {job_state}.")

        # The job exists and is not finished. So return the currentUrl,
        # or - in case that is not defined - the startUrl value.
        self.cur.execute('SELECT COALESCE(currentUrl, startUrl) ' +
                         'FROM jobs ' +
                         'WHERE jobName = %s;',
                         (job_name, ))
        return self.cur.fetchone()[0]  # type: ignore[index]

    def mark_as_finished(self,
                         job_name: str) -> None:
        """ Mark a crawl job as finished. """
        if not job_name:
            raise ValueError('Missing job_name')
        job_name = job_name.strip()
        affected_rows = self.cur.execute('UPDATE jobs SET ' +
                                         'finished = CURRENT_TIMESTAMP() ' +
                                         'WHERE jobName = %s;',
                                         (job_name, ))
        if affected_rows == 0:
            raise ValueError('A job with this name is not known.')
        logging.debug('Marked job %s as finished.', job_name)
