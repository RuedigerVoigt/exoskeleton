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


if __name__ == "__main__":
    unittest.main()
