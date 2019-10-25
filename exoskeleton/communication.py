#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# python standard libraries:
from collections import Counter
import smtplib
from email.message import EmailMessage
import logging
import textwrap


def send_mail(recipient: str,
              sender: str,
              message_subject: str,
              message_text: str):
    u"""Inform the recipient about milestones or
        important events via email. """

    if recipient == '':
        logging.error('Cannot send mail as no recipient is supplied!')

    if message_subject == '':
        logging.error('Mails need a subject line. Otherwise they' +
                      'most likely will be classified as spam.')

    if message_text == '':
        logging.error('No mail content supplied.')

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
