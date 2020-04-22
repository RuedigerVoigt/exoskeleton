# Handling Pages

The Queue is the central concept behind exoskeleton. With the exception of `return_page_code()` for throwaway information, you always add tasks to the queue.

Exoskeleton then processes that queue - sometimes slow but steady. If a server does not respond, it automatically picks another task and retries that failed one later.

## Downloading Files

```python
def add_file_download(url: str,
                      labels: set = None):
```

Then you [created the bot](create-a-bot.md) you set two parameters:
* `filename_prefix`, and
* `target_directory`

So if you pass an URL to `add_file_download()` and start the processing, exoskeleton will download the file into the specified directory. The filename will consist of the chosen prefix and the alphanumeric UUID used in the database to identify the document version.

Sometimes the URL does not contain the appropriate file ending. This is very often the case if you process a homepage and have an URL like `https://www.example.com` instead of `https://www.example.com/index.html`. As this case is so common, exoskeleton analyzes the HTTP header send by the server. So if the server announces the mime type 'text/html', the file will be stored with the file ending `.html` and accordingly with other file types.


## Saving the Page Code

Sometimes the code of a page is relevant. With `add_save_page_code()` this will not be saved in a file, but *stored in the database*.

The option `prettify_html` uses lxml to fix broken HTML code. It should not be used with an XHTML or XML file. The function accepts a set of [labels](versions-and-labels.md "How to use labels").

```python
def add_save_page_code(url: str,
                       labels: set = None,
                       prettify_html: bool = False):
```

## Saving a PDF version of a Web Site

```python
def add_page_to_pdf(url: str,
                    labels: set = None):
```

You can add a set of [labels](versions-and-labels.md "How to use labels").

This feature uses headless Chrome / Chromium to save a website as PDF. For it to work you need to set the path to the program.

**BEWARE**: *This function is not reliable for every website. Chrome / Chromium does not remember web cookies in headless mode. As a result, some websites black out the whole page and only display a cookie / data privacy dialogue and only this gets stored.*


> :arrow_right: **[Now learn how to steer the bot's behavior](behavior-settings.md)**