#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""

~~~~~~~~~~~~~~~~~~~~~

"""
# standard library:
import logging

import requests


class ExoActions:
    """Manage actions (i.e. interactions with servers)
       except remote control of the chromium browser. """

    def __init__(self,
                 stats_manager_object,
                 user_agent: str,
                 connection_timeout: int):
        """ """
        self.stats = stats_manager_object
        self.user_agent = user_agent
        self.connection_timeout = connection_timeout

    def return_page_code(self,
                         url: str):
        """Directly return a page's code. Do *not* store it in the database."""
        if url == '' or url is None:
            raise ValueError('Missing url')
        url = url.strip()

        try:
            r = requests.get(url,
                             headers={"User-agent": str(self.user_agent)},
                             timeout=self.connection_timeout,
                             stream=False
                             )

            if r.status_code == 200:
                return r.text
            else:
                raise RuntimeError('Cannot return page code')

        except TimeoutError:
            logging.error('Reached timeout.', exc_info=True)
            self.stats.update_host_statistics(url, 0, 1, 0, 0)
            raise

        except ConnectionError:
            logging.error('Connection Error', exc_info=True)
            self.stats.update_host_statistics(url, 0, 1, 0, 0)
            raise

        except Exception:
            logging.exception('Exception while trying to get page-code',
                              exc_info=True)
