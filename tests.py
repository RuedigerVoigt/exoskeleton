#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest

from exoskeleton import checks as checks
from exoskeleton import communication as communication
from exoskeleton import helpers as helpers
from exoskeleton import utils as utils


class BotTest(unittest.TestCase):

    def test_check_email_format(self):
        self.assertTrue(checks.check_email_format('test@example.com'))
        self.assertTrue(checks.check_email_format('test@example-example.com'))
        self.assertTrue(checks.check_email_format('test@example.co.uk'))
        self.assertTrue(checks.check_email_format('  test@example.com  '))
        self.assertTrue(checks.check_email_format('test+filter@example.com'))
        self.assertFalse(checks.check_email_format('@example.com'))
        self.assertFalse(checks.check_email_format('test@@example.com'))
        self.assertFalse(checks.check_email_format('test@example.'))

    def test_check_hash_algo(self):
        self.assertRaises(ValueError, checks.check_hash_algo, 'md5')
        self.assertRaises(ValueError, checks.check_hash_algo, 'sha1')
        self.assertTrue(checks.check_hash_algo('sha224'))
        self.assertTrue(checks.check_hash_algo('sha256'))
        self.assertTrue(checks.check_hash_algo('sha512'))
        self.assertRaises(ValueError,
                          checks.check_hash_algo,
                          'NonExistentMethod')

    def test_validate_port(self):
        self.assertEqual(checks.validate_port(3306), 3306)
        self.assertEqual(checks.validate_port(5432), 5432)
        self.assertRaises(ValueError, checks.validate_port, 65537)
        self.assertRaises(ValueError, checks.validate_port, -1)

    def test_validate_aws_s3_bucket_name(self):
        self.assertTrue(checks.validate_aws_s3_bucket_name('abc'))
        # too short
        self.assertFalse(checks.validate_aws_s3_bucket_name('ab'))
        # too long
        self.assertFalse(checks.validate_aws_s3_bucket_name(
            'iekoht9choofe9eixeeseizoo0iuzos1ibeepae7phee3aeghaif7shal9kiepiy'))
        # Ipv4 address
        self.assertFalse(checks.validate_aws_s3_bucket_name('127.0.0.1'))
        # invalid characters
        self.assertFalse(checks.validate_aws_s3_bucket_name(
            'iekoht9choofe9ei_xeeseizo'))
        self.assertFalse(checks.validate_aws_s3_bucket_name('iekoh#xeeseizo'))
        self.assertFalse(checks.validate_aws_s3_bucket_name('ab$$c'))
        self.assertFalse(checks.validate_aws_s3_bucket_name('ABc'))
        # starting with lowercase letter or number
        self.assertFalse(checks.validate_aws_s3_bucket_name('-abc'))
        # prefectly fine at max length
        self.assertTrue(checks.validate_aws_s3_bucket_name(
            'iekoht9choofe9eixeeseizoo0iuzos1ibeepae7phee3aeghai7shal9kiepiy'))
        # containing dots
        self.assertTrue(checks.validate_aws_s3_bucket_name(
            'iekoht9choofe.eixeeseizoo0iuzos1ibee.pae7ph'))

    def test_determine_file_extension(self):
        # URL hint matches server header
        self.assertEqual(utils.determine_file_extension(
            'https://www.example.com/example.pdf',
            'application/pdf'), '.pdf')
        # URL does not provide a hint, but the HTTP header does
        self.assertEqual(utils.determine_file_extension(
            'https://www.example.com/',
            'text/html'), '.html')
        # no server header, but hint in URL
        self.assertEqual(utils.determine_file_extension(
            'https://www.example.com/example.pdf', ''), '.pdf')
        # no hint at all
        self.assertEqual(utils.determine_file_extension(
            'https://www.example.com/', ''), '.unknown')
        # malformed server header and no hint in the URL
        self.assertEqual(utils.determine_file_extension(
            'https://www.example.com/', 'malformed/nonexist'), '.unknown')
        # text/plain
        self.assertEqual(utils.determine_file_extension(
            'https://www.example.com/test.txt', 'text/plain'), '.txt')

    def test_convert_to_set(self):
        # single string with multiple characters
        # (wrong would be making each character into an element)
        self.assertEqual(utils.convert_to_set('abc'), {'abc'})
        # list with duplicates to set
        self.assertEqual(utils.convert_to_set(
                         ['a', 'a', 'b', 'c']),
                         {'a', 'b', 'c'})
        # tuple with duplicates
        self.assertEqual(utils.convert_to_set(
                         ('a', 'a', 'b', 'c')),
                         {'a', 'b', 'c'})
        # set should return unchanged
        self.assertEqual(utils.convert_to_set(
                         {'a', 'b', 'c'}),
                         {'a', 'b', 'c'})
        # unsupported data type integer
        self.assertRaises(TypeError, utils.convert_to_set, 3)

    def test_send_mail(self):
        # wrong values for recipient
        self.assertRaises(ValueError, communication.send_mail, '',
                          'sender@example.com', 'subject', 'messageText')
        self.assertRaises(ValueError, communication.send_mail, None,
                          'sender@example.com', 'subject', 'messageText')
        self.assertRaises(ValueError, communication.send_mail, 'invalid',
                          'sender@example.com', 'subject', 'messageText')
        # wrong values for sender
        self.assertRaises(ValueError, communication.send_mail,
                          'recipient@example.com', '',
                          'subject', 'messageText')
        self.assertRaises(ValueError, communication.send_mail,
                          'recipient@example.com', None,
                          'subject', 'messageText')
        self.assertRaises(ValueError, communication.send_mail,
                          'recipient@example.com', 'invalid',
                          'subject', 'messageText')
        # missing subject
        self.assertRaises(ValueError, communication.send_mail,
                          'recipient@example.com', 'sender@example.com',
                          '', 'messageText')
        self.assertRaises(ValueError, communication.send_mail,
                          'recipient@example.com', 'sender@example.com',
                          None, 'messageText')
        # missing mail text
        self.assertRaises(ValueError, communication.send_mail,
                          'recipient@example.com', 'sender@example.com',
                          'subject', '')
        self.assertRaises(ValueError, communication.send_mail,
                          'recipient@example.com', 'sender@example.com',
                          'subject', None)

    def test_check_url_format(self):
        # missing scheme
        self.assertFalse(checks.check_url_format('noscheme.example.com'))
        # unsuported scheme
        self.assertFalse(checks.check_url_format('ftp://example.com'))
        # scheme with typo (one instead of two slashes)
        self.assertFalse(checks.check_url_format('https:/example.com'))
        # valid URLs
        self.assertTrue(
            checks.check_url_format('https://example.com'))
        self.assertTrue(
            checks.check_url_format('https://subdomain.example.com'))
        self.assertTrue(
            checks.check_url_format('https://example.com/index.php?id=42'))

    def test_date_en_long_to_iso(self):
        # valid input:
        self.assertEqual(
            helpers.date_en_long_to_iso('Jul. 4, 1776'), '1776-07-04')
        self.assertEqual(
            helpers.date_en_long_to_iso('May 8, 1945'), '1945-05-08')
        self.assertEqual(
            helpers.date_en_long_to_iso('May 08, 1945'), '1945-05-08')
        self.assertEqual(
            helpers.date_en_long_to_iso('October 3, 1990'), '1990-10-03')
        self.assertEqual(
            helpers.date_en_long_to_iso('November 03, 2020'), '2020-11-03')
        # messed up whitespace:
        self.assertEqual(
            helpers.date_en_long_to_iso('Jul. 4,      1776'), '1776-07-04')
        self.assertEqual(
            helpers.date_en_long_to_iso('Jul. 4,1776'), '1776-07-04')
        self.assertEqual(
            helpers.date_en_long_to_iso('   Jul. 4, 1776  '), '1776-07-04')
        self.assertEqual(
            helpers.date_en_long_to_iso('Jul.    4, 1776'), '1776-07-04')
        # grammatically incorrect, but clear:
        self.assertEqual(
            helpers.date_en_long_to_iso('May 8th, 1945'), '1945-05-08')
        # upper and lower case:
        self.assertEqual(
            helpers.date_en_long_to_iso('jul. 4, 1776'), '1776-07-04')
        self.assertEqual(
            helpers.date_en_long_to_iso('JUL. 4, 1776'), '1776-07-04')
        # leap year:
        self.assertEqual(
            helpers.date_en_long_to_iso('February 29, 2020'), '2020-02-29')
        # non-existing date:
        self.assertRaises(ValueError,
                          helpers.date_en_long_to_iso,
                          'February 30, 2020')


if __name__ == "__main__":
    unittest.main()
