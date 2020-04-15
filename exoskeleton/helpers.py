#!/usr/bin/env python3
# -*- coding: utf-8 -*-

u""" Helper functions to convert between formats, ... Everything
not within the main scope of exoskeleton, but helpful for bots. """

import datetime
import logging
import re
from typing import Union


def check_date_validity(year: Union[int, str],
                        month: Union[int, str],
                        day: Union[int, str]) -> bool:
    u"""Check if a date is valid i.e exists in the calendar. """
    try:
        # int() will convert something like '01' to 1
        year = int(year)
        month = int(month)
        day = int(day)
    except ValueError:
        logging.error('Could not convert date parts to integer.')
        return False

    try:
        datetime.datetime(year, month, day)
    except ValueError:
        logging.error('Provided date does not exist in the calendar.')
        return False
    return True


def date_en_long_to_iso(date_string: str) -> str:
    u""" Changes long format English date to short form date. """
    date_string = date_string.strip()
    regex_long_date_en = re.compile("(?P<monthL>[a-zA-Z\.]{3,9})\s+(?P<day>\d{1,2})(th)?,\s*(?P<year>\d\d\d\d)")
    match = re.search(regex_long_date_en, date_string)
    matchYear = match.group('year')
    matchMonth = match.group('monthL')

    # add a zero to day if <10
    matchDay = match.group('day')
    if(len(matchDay) == 1):
        matchDay = '0' + matchDay
    months = {
        'January': '01',
        'Jan.': '01',
        'February': '02',
        'Feb.': '02',
        'March': '03',
        'Mar.': '03',
        'April': '04',
        'Apr.': '04',
        'May': '05',
        'June': '06',
        'Jun.': '06',
        'July': '07',
        'Jul.': '07',
        'August': '08',
        'Aug.': '08',
        'September': '09',
        'Sep.': '09',
        'October': '10',
        'Oct.': '10',
        'November': '11',
        'Nov.': '11',
        'December': '12',
        'Dec.': '12'
        }
    matchMonth = months[str(matchMonth).lower().capitalize()]
    if check_date_validity(matchYear, matchMonth, matchDay):
        return(f"{matchYear}-{matchMonth}-{matchDay}")
    else:
        raise ValueError('Provided date is invalid.')
