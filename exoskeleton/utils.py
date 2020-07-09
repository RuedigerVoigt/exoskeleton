#!/usr/bin/env python3
# -*- coding: utf-8 -*-

u""" Utility functions to interact with files, ... """

# python standard library:
import logging
import pathlib

# 3rd party libraries:
from bs4 import BeautifulSoup  # type: ignore


def get_file_size(file_path: pathlib.Path) -> int:
    u"""file size in bytes."""
    try:
        return file_path.stat().st_size
    except Exception:
        logging.error('Cannot get file size', exc_info=True)
        raise


def prettify_html(content: str) -> str:
    u"""Parse the HTML => add a document structure if needed
        => Encode HTML-entities and the document as Unicode (UTF-8).
        Only use for HTML, not XML."""

    # Empty elements are not removed as they might
    # be used to find specific elements within the tree.

    content = BeautifulSoup(content, 'lxml').prettify()

    # TO DO: Add functionality to add a base URL
    # See issue #13.

    return content
