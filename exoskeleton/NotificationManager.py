#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Notification Management for the Exoskeleton Crawler Framework
~~~~~~~~~~~~~~~~~~~~~

"""
# standard library:
from collections import defaultdict
import logging

# external dependencies:
import bote
import userprovided


class NotificationManager:
    u"""Notification management for the exoskeleton crawler framework.
        At the moment this sends out email, but it may support other
        notification methods in future versions of exoskeleton."""

    def __init__(self,
                 project_name: str,
                 mail_settings: dict,
                 mail_behavior: dict):
        u"""Sets defaults"""

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

            self.do_send_start_msg = mail_behavior.get('send_start_msg', True)
            userprovided.parameters.enforce_boolean(
                self.do_send_start_msg, 'send_start_msg')

            if self.do_send_start_msg:
                self.send_msg('start')

            self.do_send_finish_msg = mail_behavior.get(
                'send_finish_msg', False)
            userprovided.parameters.enforce_boolean(
                self.do_send_finish_msg, 'send_finish_msg')

            if self.send_finish_msg:
                logging.info('Will send notification once the bot is done.')

    def send_msg(self,
                 reason: str):
        u"""Send a prepared message if the bot is configured
            and able to do so."""
        messages = {
            'start': {
                'subject': f"Project {self.project_name} just started.",
                'body': ("The bot just started. This is a notification " +
                         "to inform you and to check the mail settings.")
                         },
            'abort_lost_db': {
                'subject': f"Project {self.project_name} ABORTED",
                'body': ("The bot lost the database connection and could " +
                         "not restore it.")
                         }
                }

        if reason == 'start' and self.do_send_start_msg:
            self.mailer.send_mail(
                messages['start']['subject'],
                messages['start']['body'])
            logging.info("Just sent a notification email. If the " +
                         "receiving server uses greylisting, " +
                         "this may take some minutes.")

    def send_milestone_msg(self,
                           processed: int,
                           remaining: int,
                           time_to_finish_seconds: int):
        subject = (f"Project {self.project_name} Milestone: " +
                   f"{processed} processed")
        body = (f"{processed} processed.\n" +
                f"{remaining} tasks remaining in the queue.\n" +
                f"Estimated time to complete queue: " +
                f"{time_to_finish_seconds} seconds.\n")
        self.mailer.send_mail(subject, body)

    def send_finish_msg(self,
                        num_permanent_errors: int):
        if self.send_mails and self.do_send_finish_msg:
            subject = f"{self.project_name}: queue empty / bot stopped"
            body = (f"The queue is empty. The bot " +
                    f"{self.project_name} stopped as configured. " +
                    f"{num_permanent_errors} errors.")
            self.mailer.send_mail(subject, body)

    def send_custom_msg(self,
                        subject: str,
                        body: str):
        u"""Send a custom message if the bot is configured
            and able to do so."""
        if self.send_mails:
            self.mailer.send_mail(subject, body)