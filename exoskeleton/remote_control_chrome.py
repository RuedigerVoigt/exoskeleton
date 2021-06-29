#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Control an instance of headless Chrome.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
Released under the Apache License 2.0
"""
# standard library:
import logging
import pathlib
import shutil
import subprocess

from exoskeleton import error_manager
from exoskeleton import exo_url
from exoskeleton import statistics_manager


class RemoteControlChrome:
    """Using headless Chrome instead of Selenium for the following reasons:
       * Users do not need to install and update a version of chromedriver
         matching the browser.
       * It is fast and does not open a GUI.
       * Selenium has no built in command for PDF export, but needs
         to operate the dialog. That is far more likely to break.
       BEWARE: Some cookie-popups blank out the page and all what is stored
               is the dialogue."""

    SUPPORTED_BROWSERS = {'google-chrome', 'chrome', 'chromium',
                          'chromium-browser'}

    def __init__(
            self,
            browser_name: str,
            crawling_error_manager_object: error_manager.CrawlingErrorManager,
            stats_manager_object: statistics_manager.StatisticsManager
            ) -> None:
        """Check the path of the executable and if it is supported. """

        self.browser_name = ''
        self.browser_present = False
        if not browser_name:
            logging.info('Provide a browser name if you want to save as PDF.')
        else:
            self.browser_name = browser_name.strip()
            if not self.check_executable_existence(self.browser_name):
                logging.exception("Browser not found in the path. Check name.")
                self.suggest_executables()
                raise ValueError(f"Browser {self.browser_name} not in path!")
            if self.check_browser_support(self.browser_name):
                self.browser_present = True
        self.errorhandling = crawling_error_manager_object
        self.stats = stats_manager_object

    @staticmethod
    def check_executable_existence(browser_name: str) -> bool:
        "See if the executable name provided by the setup is in the path."
        return bool(shutil.which(browser_name))

    def suggest_executables(self) -> None:
        """Check for all supported browsers if they are available on the
           system. If so tell the user about it with a log message (info)."""
        for browser in self.SUPPORTED_BROWSERS:
            if self.check_executable_existence(browser):
                logging.info('Found supported browser in PATH to save pdf: %s',
                             browser)

    def check_browser_support(self,
                              browser_name: str) -> bool:
        "Return True if the browser is supported. Else raise an exception."
        if browser_name.lower() in self.SUPPORTED_BROWSERS:
            logging.info('Browser supported.')
            return True
        msg = (f"{browser_name} is unsupported or unknown. You must use " +
               "Chromium  or Google Chrome for the 'save as PDF' feature.")
        logging.exception(msg)
        self.suggest_executables()
        raise ValueError(msg)

    def page_to_pdf(self,
                    url: exo_url.ExoUrl,
                    file_path: pathlib.Path,
                    queue_id: str) -> None:
        """ Uses the Google Chrome or Chromium browser in headless mode
            to print the page to PDF and stores that.
            BEWARE: Some cookie-popups blank out the page and all what is
            stored is the dialogue."""

        if not self.browser_present:
            raise ValueError('As you have not provided a valid name / path ' +
                             'of the Chromium or Chrome process to use, you ' +
                             'cannot save a page in PDF format.')

        try:
            # Using the subprocess module as it is part of the
            # standard library and set up to replace os.system
            # and os.spawn!
            subprocess.run([self.browser_name,
                            "--headless",
                            "--new-windows",
                            "--disable-gpu",
                            "--account-consistency",
                            # No additional quotation marks around the path:
                            # subprocess does the necessary escaping!
                            f"--print-to-pdf={file_path}",
                            str(url)],
                           shell=False,
                           timeout=30,
                           check=True)

            logging.debug('PDF of page saved to disk')

        except subprocess.TimeoutExpired:
            logging.exception(
                'Cannot create PDF due to subprocess timeout.', exc_info=True)
            self.errorhandling.add_crawl_delay(queue_id, 4)
            self.stats.log_temporary_problem(url)
        except subprocess.CalledProcessError:
            logging.exception('Process Error: cannot create PDF.',
                              exc_info=True)
            self.errorhandling.add_crawl_delay(queue_id, 5)
            self.stats.log_permanent_error(url)
        except (Exception, subprocess.SubprocessError):  # pylint: disable=broad-except
            logging.exception('Exception.', exc_info=True)
            self.errorhandling.add_crawl_delay(queue_id, 0)
            self.stats.log_temporary_problem(url)
