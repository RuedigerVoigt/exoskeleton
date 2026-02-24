"""
The class CrawlingErrorManager manages errors that occur while
crawling. It marks permanent errors and adds delays.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2025 Rüdiger Voigt and contributors:
Released under the Apache License 2.0
"""
# standard library:
from datetime import datetime, timedelta
from hashlib import sha256
import logging

# external dependencies:
from sqlalchemy.orm import Session
import userprovided

from exoskeleton import database_connection
from exoskeleton import models

logger = logging.getLogger(__name__)


class CrawlingErrorManager:
    """Manage errors that occur while crawling."""

    # If there is a temporary error, exoskeleton delays the next try,
    # until the configured maximum of tries is reached.
    # The time between tries is definied here to be able to overwrite it
    # in case of an automatic tests to avoid multi-hour runtimes.
    # Steps: 1/4h, 1/2h, 1h, 3h, 6h
    DELAY_TRIES = (900, 1800, 3600, 10800, 21600)

    def __init__(self,
                 db_connection: database_connection.DatabaseConnection,
                 queue_max_retries: int,
                 rate_limit_wait_seconds: int) -> None:
        self.db_connection = db_connection
        self.session: Session = db_connection.get_session()
        # Maximum number of retries if downloading a page/file failed:
        self.queue_max_retries: int = queue_max_retries
        self.queue_max_retries = userprovided.parameters.int_in_range(
            "queue_max_retries", self.queue_max_retries, 0, 10, 3)

        # Seconds to wait before contacting the same FQDN again, after the bot
        # hit a rate limit. Defaults to 1860 seconds (i.e. 31 minutes):
        self.rate_limit_wait: int = rate_limit_wait_seconds

    def add_crawl_delay(self,
                        queue_id: str,
                        error_type: int) -> None:
        """In case of a timeout or a temporary error increment the counter for
        the number of tries by one. If the configured maximum of tries
        was reached, mark it as a permanent error. Otherwise add a delay,
        so exoskelton does not try the same task again. As multiple tasks
        may affect the same URL, the delay is added to all of them."""
        wait_time = 0

        # Increase the tries counter and get the new count
        self.session.query(models.Queue).filter(
            models.Queue.id == queue_id
        ).update({
            models.Queue.numTries: models.Queue.numTries + 1
        }, synchronize_session=False)
        self.session.commit()

        # Get the updated count
        queue_item = self.session.query(models.Queue.numTries).filter(
            models.Queue.id == queue_id
        ).first()
        num_tries = int(queue_item[0]) if queue_item else 0

        # Does the number of tries exceed the configured maximum?
        if num_tries == self.queue_max_retries:
            # This is treated as a *permanent* failure!
            logger.error('Giving up: too many tries for task %s', queue_id)
            self.mark_permanent_error(queue_id, 3)
            return

        logger.info('Adding crawl delay to task %s', queue_id)
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

        # Get the URL hash for this queue item
        queue_item = self.session.query(models.Queue.urlHash).filter(  # type: ignore[assignment]
            models.Queue.id == queue_id
        ).first()

        if not queue_item:
            logger.warning("Queue ID '%s' not found, skipping crawl delay.", queue_id)
            return

        url_hash = queue_item[0]

        # Update all queue items with same URL hash
        delay_until = datetime.now() + timedelta(seconds=wait_time)
        self.session.query(models.Queue).filter(
            models.Queue.urlHash == url_hash
        ).update({
            models.Queue.delayUntil: delay_until
        }, synchronize_session=False)

        # Mark the specific item with error
        self.session.query(models.Queue).filter(
            models.Queue.id == queue_id
        ).update({
            models.Queue.causesError: error_type
        }, synchronize_session=False)

        self.session.commit()

    def mark_permanent_error(self,
                             queue_id: str,
                             error: int) -> None:
        """ Mark task in queue that causes a *permanent* error.
            Without this exoskeleton would try to execute it again."""
        result = self.session.query(models.Queue).filter(
            models.Queue.id == queue_id
        ).update({
            models.Queue.causesError: error
        }, synchronize_session=False)
        self.session.commit()

        if result == 0:
            raise ValueError(f"Queue ID '{queue_id}' not found")

        logger.info('Marked task %s as causing a permanent error.', queue_id)

    def forget_specific_error(self,
                              specific_error: int) -> None:
        """Treat all queued tasks, that are marked to cause a *specific*
           error, as if they are new tasks by removing that mark and any delay.
           The number of the error has to correspond to the errorType
           database table."""
        self.session.query(models.Queue).filter(
            models.Queue.causesError == specific_error
        ).update({
            models.Queue.causesError: None,
            models.Queue.numTries: 0,
            models.Queue.delayUntil: None
        }, synchronize_session=False)
        self.session.commit()

    def forget_temporary_errors(self) -> None:
        """Treat all queued tasks, that are marked to cause a *temporary*
        error, as if they are new tasks by removing that mark and any delay."""
        # Get IDs of temporary errors
        temp_error_ids = self.session.query(models.ErrorType.id).filter(
            models.ErrorType.permanent.is_(False)
        ).all()
        temp_error_ids = [id[0] for id in temp_error_ids]

        # Update queue items with those errors
        if temp_error_ids:
            self.session.query(models.Queue).filter(
                models.Queue.causesError.in_(temp_error_ids)
            ).update({
                models.Queue.causesError: None,
                models.Queue.numTries: 0,
                models.Queue.delayUntil: None
            }, synchronize_session=False)
            self.session.commit()

    def forget_permanent_errors(self) -> None:
        """Treat all queued tasks, that are marked to cause a *permanent*
           error, as if they are new tasks by removing that mark and
           any delay."""
        # Get IDs of permanent errors
        perm_error_ids = self.session.query(models.ErrorType.id).filter(
            models.ErrorType.permanent.is_(True)
        ).all()
        perm_error_ids = [id[0] for id in perm_error_ids]

        # Update queue items with those errors
        if perm_error_ids:
            self.session.query(models.Queue).filter(
                models.Queue.causesError.in_(perm_error_ids)
            ).update({
                models.Queue.causesError: None,
                models.Queue.numTries: 0,
                models.Queue.delayUntil: None
            }, synchronize_session=False)
            self.session.commit()

    def forget_all_errors(self) -> None:
        """Treat all queued tasks, that are marked to cause any type of
           error, as if they were new tasks by removing that mark and any
           task specific delay.
           However, this does not remove delays due to rate limit on a per host
           basis. Use corresponding functions to remove those."""
        self.session.query(models.Queue).update({
            models.Queue.causesError: None,
            models.Queue.numTries: 0,
            models.Queue.delayUntil: None
        }, synchronize_session=False)
        self.session.commit()

    def add_rate_limit(self,
                       fqdn: str) -> None:
        """If a bot receives the statuscode 429 ('too many requests') it hit
           a rate limit. Adding the fully qualified domain name to the rate
           limit list, ensures that this FQDN is not contacted for a
           predefined time."""
        msg = (f"Bot hit a rate limit with {fqdn}. Will not try to " +
               f"contact this host for {self.rate_limit_wait} seconds.")
        logger.error(msg)

        fqdn_hash = sha256(fqdn.encode('utf-8')).hexdigest()
        no_contact_until = datetime.now() + timedelta(seconds=self.rate_limit_wait)

        rate_limit = self.session.query(models.RateLimit).filter(
            models.RateLimit.fqdnHash == fqdn_hash
        ).first()

        if rate_limit:
            rate_limit.noContactUntil = no_contact_until  # type: ignore[assignment]
        else:
            rate_limit = models.RateLimit(
                fqdnHash=fqdn_hash,
                fqdn=fqdn,
                noContactUntil=no_contact_until
            )
            self.session.add(rate_limit)

        self.session.commit()

    def forget_specific_rate_limit(self,
                                   fqdn: str) -> None:
        "Forget that the bot hit a rate limit for a specific FQDN."
        fqdn_hash = sha256(fqdn.encode('utf-8')).hexdigest()
        self.session.query(models.RateLimit).filter(
            models.RateLimit.fqdnHash == fqdn_hash
        ).delete(synchronize_session=False)
        self.session.commit()

    def forget_all_rate_limits(self) -> None:
        """Forget all rate limits the bot hit."""
        self.session.query(models.RateLimit).delete(synchronize_session=False)
        self.session.commit()
