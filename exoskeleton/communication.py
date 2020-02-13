#!/usr/bin/env python3
# -*- coding: utf-8 -*-

u""" Send messages to the admin """

import smtplib
from email.message import EmailMessage
import logging
import textwrap
import exoskeleton.checks as checks


def send_mail(recipient: str,
              sender: str,
              message_subject: str,
              message_text: str):
    u"""Inform the recipient about milestones or
        important events via email. """

    if recipient == '' or recipient is None or not checks.check_email_format(recipient):
        raise ValueError('Cannot send mail as no recipient is supplied!')

    if sender == '' or sender is None or not checks.check_email_format(sender):
        raise ValueError('Cannot send mail as no sender is supplied!')

    if message_subject == '' or message_subject is None:
        raise ValueError('Mails need a subject line. Otherwise they' +
                         'most likely will be classified as spam.')

    if message_text == '' or message_text is None:
        raise ValueError('No mail content supplied.')

    wrap = textwrap.TextWrapper(width=80)

    # To preserve intentional linebreaks, the text is
    # wrapped linewise.
    wrapped_text = ''
    for line in str.splitlines(message_text):
        wrapped_text += wrap.fill(line) + "\n"

    try:
        msg = EmailMessage()
        msg.set_content(wrapped_text)
        msg['Subject'] = message_subject
        msg['From'] = sender
        msg['To'] = recipient

        s = smtplib.SMTP('localhost')
        s.send_message(msg)
        s.quit()
    except smtplib.SMTPAuthenticationError:
        logging.exception('SMTP authentication failed. ' +
                          'Please check username / passphrase.')
    except Exception:
        logging.exception('Problem sending Mail!', exc_info=True)
        raise
    return
