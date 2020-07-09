# Parse Search Results

If you need to download a huge number of files it is most likely that you use the search function on the site, go through multiple result pages and then download files for that match. Often you will do another search and repeat that.

## Going through SERPs

This is (simplified) how a typical result page might look like:
```html
<ul class="searchresults">
<li><a href="example_1.pdf">Example</a></li>
<li><a href="example_2.pdf">Example</a></li>
<li><a href="example_3.pdf">Example</a></li>
<li><a href="example_4.pdf">Example</a></li>
<li>...</li>
</ul>
<a class="next" href="serp-page2.html">next</a>
```

You would need to find all result URLs (`example_1.pdf`, `example_2.pdf`, ...) and then identify the link to the next page. You would repeat that until there is no link to the next page as you reached the end.

We will use the excellent [Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/ "beautiful soup project homepage") package to analyze the content of the pages. Exoskeleton will store the progress and the links to download.

The exoskeleton function `return_page_code()` gives back the code of a page without storing it in the database. As we have no further need for the page code, we use that function here.

There is a high risk to run into a rate limit on result page 400 or 500. You do not want to start again from scratch. Therefore, we define a job to store the progress. You need to know the following four exoskeleton functions:

`job_define_new(job_name, start_url)`: defines a job. It stores the given URL in the database. So it can be accessed even after the scripts fail.

`job_update_current_url(job_name, current_url)`: overwrites the stored URL / start URL. Do this every time you completely analyzed a search result page.

`job_get_current_url(job_name)`: looks up the current URL, that is the state of progress.

`job_mark_as_finished(job_name)`: after you reached the last page, mark the job as finished.

So, this is a way to loop through all search result pages:

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import exoskeleton
import logging
logging.basicConfig(level=logging.INFO)

# Create a Bot
exo = exoskeleton.Exoskeleton(
    database_settings={'database': credentials.db,
                       'username': credentials.user,
                       'passphrase': credentials.passphrase},
    filename_prefix='TEST_',
    target_directory='/home/yourusername/testfolder/'
)


def crawl(job: str,
          url_base: str = ''):
    u"""loop until next_page is undefined."""

    while True:

        try:
            # At which page are we?
            page_to_crawl = exo.job_get_current_url(job)
        except RuntimeError:
            # That means the job is already finished
            # leave the while loop
            break

        # directly get the page content and parse it with lxml
        soup = BeautifulSoup(exo.return_page_code(page_to_crawl), 'lxml')

        # extract all relevant links to detail pages
        urls = soup.select("ul.searchresults a")

        if urls:
            # loop over all URLs and add the base
            # # then add them to the queue
            for i in urls:
                full_url = f"{url_base}{i['href']}"
                print(full_url)
                # This function adds the task "download the file"
                # to the queue. We add the name of the job as a label.
                exo.add_file_download(full_url, {job})

        try:
            # check whether a next page is defined for the SERPs
            next_page = soup.select("a.next")

            if next_page:
                # next_page is definied, get first element:
                next_page = next_page[0]

                # combine base and content of href attribute:
                next_page = f"{url_base}{next_page['href']}"

                exo.job_update_current_url(job, next_page)
                exo.random_wait()
            else:
                # next_page is *not* defined
                exo.job_mark_as_finished(job)
                break # finish while loop
        except:
            raise
            # ends up here if next page is undefined
    print('Crawl done')


exo.job_define_new('Example Job',
                   'https://www.example.com?keyword=example')

# Actually start the crawl through the SERPs
crawl('Example Job', 'https://www.example.com/')

# now that the crawling is done, proceed
# and download all files in the queue:
exo.process_queue()
```

> :arrow_right: **[Learn how to avoid storing duplicates](avoiding-duplicates.md)**
