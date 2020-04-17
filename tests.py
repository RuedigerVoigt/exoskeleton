#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest

from exoskeleton import checks as checks
from exoskeleton import communication as communication
from exoskeleton import utils as utils


class BotTest(unittest.TestCase):

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


if __name__ == "__main__":
    unittest.main()
