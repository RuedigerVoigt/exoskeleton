#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Helper Functions for Exoskeleton Crawler Framework
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2021 Rüdiger Voigt:
Released under the Apache License 2.0
"""


from bs4 import BeautifulSoup  # type: ignore


def prettify_html(content: str) -> str:
    """Only use for HTML, not XML.
       Parse the HTML:
         * add a document structure if needed
         * Encode HTML-entities and the document as Unicode (UTF-8).
         * Empty elements are NOT removed as they might be used to find
           specific elements within the tree.
    """
    content = BeautifulSoup(content, 'lxml').prettify()
    return content


def strip_code(content: str) -> str:
    "Remove code tags from HTMl and return the text"
    soup = BeautifulSoup(content, 'lxml')
    return soup.get_text()
