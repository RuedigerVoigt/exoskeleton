#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Notification Management for the Exoskeleton Crawler Framework
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2021 Rüdiger Voigt
Released under the Apache License 2.0
"""
from collections import defaultdict  # noqa # pylint: disable=unused-import
import logging

import bote
import userprovided


class NotificationManager:
    """Notification management for the exoskeleton crawler framework.
       At the moment this sends out email, but it may support other
       notification methods in future versions."""

    def __init__(self,
                 project_name: str,
                 mail_settings: dict,
                 mail_behavior: dict):
        """Sets defaults"""

        self.project_name = project_name

        self.send_mails: bool = False

        if not mail_settings:
            logging.info("No mail-settings: will not send any notifications.")
        else:
            self.mailer = bote.Mailer(mail_settings)
            # The constructor would have failed with exceptions,
            # if the settings were implausible:
            self.send_mails = True
            logging.info('This bot will try to send notifications.')

            if mail_behavior.get('send_start_msg', True):
                self.__send_msg_start()

            self.do_send_finish_msg = mail_behavior.get(
                'send_finish_msg', False)
            userprovided.parameters.enforce_boolean(
                self.do_send_finish_msg, 'send_finish_msg')

            if self.do_send_finish_msg is True:
                logging.info('Will send notification once the bot is done.')

    def send_milestone_msg(self,
                           processed: int,
                           remaining: int,
                           time_to_finish_seconds: int) -> None:
        """Once a milestone is reached, send an email with an estimate
           how long it will take for the bot to finish."""
        # TO DO: more precise estimate requires to account for rate limits
        subject = (
            f"Project {self.project_name} Milestone: {processed} processed")
        body = (f"{processed} processed.\n" +
                f"{remaining} tasks remaining in the queue.\n" +
                "Estimated time to complete queue: " +
                f"{round(time_to_finish_seconds / 60)} minutes.\n")
        self.mailer.send_mail(subject, body)

    def __send_msg_start(self) -> None:
        "Send a notofication about the start of the bot."
        self.mailer.send_mail(
            f"Project {self.project_name} just started.",
            ("The bot just started. This is a notification " +
             "to inform you and to check the mail settings.")
             )
        logging.debug('Sent a message announcing the start')

    def send_finish_msg(self,
                        num_permanent_errors: int) -> None:
        """If configured so, send an email once the queue is empty
           and the bot stopped."""
        if self.send_mails and self.do_send_finish_msg:
            subject = f"{self.project_name}: queue empty / bot stopped"
            body = (f"The queue is empty. The bot {self.project_name} " +
                    f"stopped as configured. {num_permanent_errors} errors.")
            self.mailer.send_mail(subject, body)

    def send_msg_abort_lost_db(self) -> None:
        "Send a message that the bot cannot connect to the database."
        self.mailer.send_mail(
            f"Project {self.project_name} ABORTED",
            ("The bot lost the database connection and could not restore it.")
             )
        logging.debug('Sent a message about lost database connection.')

    def send_custom_msg(self,
                        subject: str,
                        body: str) -> None:
        "Send a custom message if the bot is configured and able to do so."
        if self.send_mails:
            self.mailer.send_mail(subject, body)
