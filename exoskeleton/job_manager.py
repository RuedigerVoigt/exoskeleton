"""
JobManager for the exoskeleton framework.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2025 Rüdiger Voigt and contributors:
Released under the Apache License 2.0
"""
# standard library:
from hashlib import sha256
import logging

# external dependencies:
import userprovided
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError

from exoskeleton import database_connection
from exoskeleton import exo_url
from exoskeleton import models

logger = logging.getLogger(__name__)


class JobManager:
    """Jobs are used to crawl multi-page results like search engine queries.
       You provide a start URL and update it while looping through pagination.
       By doing this you can restart a paused job without starting all over."""
    # pylint: disable=raise-missing-from

    def __init__(self,
                 db_connection: database_connection.DatabaseConnection
                 ) -> None:
        "Sets defaults"
        self.db_connection = db_connection

    def define_new(self,
                   job_name: str,
                   start_url: exo_url.ExoUrl | str) -> None:
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
            with self.db_connection.session_scope() as session:
                new_job = models.Job(
                    jobName=job_name,
                    startUrl=str(start_url),
                    startUrlHash=sha256(
                        str(start_url).encode('utf-8')).hexdigest()
                )
                session.add(new_job)
            logger.debug('Defined new job.')
        except IntegrityError:
            # A job with this name already exists
            # Check if startURL is the same:
            with self.db_connection.session_scope() as session:
                existing_job = session.query(models.Job).filter(
                    models.Job.jobName == job_name
                ).first()
                if existing_job and existing_job.startUrl != str(start_url):
                    raise ValueError('A job with the identical name but ' +
                                     '*different* startURL is already defined!')
            logger.warning(
                'A job with identical name and startURL is already defined.')

    def update_current_url(self,
                           job_name: str,
                           current_url: exo_url.ExoUrl | str) -> None:
        "Set the currentUrl for a specific job. "
        if not job_name:
            raise ValueError('Provide the job name.')
        if not current_url:
            raise ValueError('Current URL must not be empty.')
        if not isinstance(current_url, exo_url.ExoUrl):
            current_url = exo_url.ExoUrl(current_url)

        with self.db_connection.session_scope() as session:
            result = session.query(models.Job).filter(
                models.Job.jobName == job_name
            ).update({
                models.Job.currentUrl: str(current_url)
            }, synchronize_session=False)

            if result == 0:
                raise ValueError('A job with this name is not known.')

    def get_current_url(self,
                        job_name: str) -> str:
        """ Returns the current URL for this job. If none is stored, this
            returns the start URL.
            Raises ValueError if the job is unknown.
            Raises RuntimeError if the job is already finished."""

        with self.db_connection.session_scope() as session:
            job = session.query(
                models.Job.finished,
                func.coalesce(
                    models.Job.currentUrl, models.Job.startUrl).label('url')
            ).filter(
                models.Job.jobName == job_name
            ).first()

        if job is None:
            raise ValueError('Job is unknown!')
        if job[0] is not None:
            # Field 0 contains the status: finished (not None) or not (None)
            raise RuntimeError(f"Job {job_name} already finished.")
        # Field 1 contains either the current or the start URL
        return str(job[1])

    def mark_as_finished(self,
                         job_name: str) -> None:
        "Mark a crawl job as finished."
        if not job_name:
            raise ValueError('Missing job_name')
        job_name = job_name.strip()

        with self.db_connection.session_scope() as session:
            result = session.query(models.Job).filter(
                models.Job.jobName == job_name
            ).update({
                models.Job.finished: func.current_timestamp()
            }, synchronize_session=False)

            if result == 0:
                raise ValueError('A job with this name is not known.')
        logger.debug('Marked job %s as finished.', job_name)
