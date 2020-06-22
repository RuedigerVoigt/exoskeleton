#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest

from exoskeleton import utils as utils


class BotTest(unittest.TestCase):

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


if __name__ == "__main__":
    unittest.main()
