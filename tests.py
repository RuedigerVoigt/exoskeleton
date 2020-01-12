#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import unittest

from exoskeleton import checks as checks
from exoskeleton import utils as utils

class BotTest(unittest.TestCase):

    def test_check_email_format(self):
        self.assertEqual(checks.check_email_format('test@example.com'), 'test@example.com')
        self.assertEqual(checks.check_email_format('  test@example.com  '), 'test@example.com')
        self.assertEqual(checks.check_email_format('test+filter@example.com'), 'test+filter@example.com')
        self.assertRaises(ValueError, checks.check_email_format, '@example.com')
        self.assertRaises(ValueError, checks.check_email_format, 'test@@example.com')
        self.assertRaises(ValueError, checks.check_email_format, 'test@example.')

    def test_check_hash_algo(self):
        self.assertEqual(checks.check_hash_algo('md5'), 'md5')
        self.assertEqual(checks.check_hash_algo('sha1'), 'sha1')
        self.assertEqual(checks.check_hash_algo('sha224'), 'sha224')
        self.assertEqual(checks.check_hash_algo('sha256'), 'sha256')
        self.assertEqual(checks.check_hash_algo('sha512'), 'sha512')
        self.assertRaises(ValueError, checks.check_hash_algo, 'NonExistentMethod')

    def test_validate_port(self):
        self.assertEqual(checks.validate_port(3306, 'mariadb'), 3306)
        self.assertEqual(checks.validate_port(5432, 'mariadb'), 5432)
        self.assertRaises(ValueError, checks.validate_port, 65537, 'mariadb')
        self.assertRaises(ValueError, checks.validate_port, -1, 'mariadb')

    def test_validate_aws_s3_bucket_name(self):
        self.assertTrue(checks.validate_aws_s3_bucket_name('abc'))
        # too short
        self.assertFalse(checks.validate_aws_s3_bucket_name('ab'))
        # too long
        self.assertFalse(checks.validate_aws_s3_bucket_name('iekoht9choofe9eixeeseizoo0iuzos1ibeepae7phee3aeghaif7shal9kiepiy'))
        # Ipv4 address
        self.assertFalse(checks.validate_aws_s3_bucket_name('127.0.0.1'))
        # invalid characters
        self.assertFalse(checks.validate_aws_s3_bucket_name('iekoht9choofe9ei_xeeseizo'))
        self.assertFalse(checks.validate_aws_s3_bucket_name('iekoh#xeeseizo'))
        self.assertFalse(checks.validate_aws_s3_bucket_name('ab$$c'))
        self.assertFalse(checks.validate_aws_s3_bucket_name('ABc'))
        # starting with lowercase letter or number
        self.assertFalse(checks.validate_aws_s3_bucket_name('-abc'))
        # prefectly fine at max length
        self.assertTrue(checks.validate_aws_s3_bucket_name('iekoht9choofe9eixeeseizoo0iuzos1ibeepae7phee3aeghai7shal9kiepiy'))
        # containing dots
        self.assertTrue(checks.validate_aws_s3_bucket_name('iekoht9choofe.eixeeseizoo0iuzos1ibee.pae7ph'))

    def test_convert_to_set(self):
        # single string with multiple characters
        # (wrong would be making each character into an element)
        self.assertEqual(utils.convert_to_set('abc'), {'abc'})
        # list with duplicates to set
        self.assertEqual(utils.convert_to_set(['a','a','b','c']), {'a','b','c'})
        # tuple with duplicates
        self.assertEqual(utils.convert_to_set(('a','a','b','c')), {'a','b','c'})
        # set should return unchanged
        self.assertEqual(utils.convert_to_set({'a','b','c'}), {'a','b','c'})
        # unsupported data type integer
        self.assertRaises(TypeError, utils.convert_to_set, 3)


if __name__ == "__main__":
    unittest.main()
