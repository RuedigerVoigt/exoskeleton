# Handling Pages

## The Queue

The Queue is the central concept behind exoskeleton. With the exception of `return_page_code()` for throwaway information, you always add tasks to the queue.

With the `process_queue()` command you tell exoskeleton to work through that queue. It tries to work on tasks in the order they were added. However, if a server does not respond or throws an error, it automatically picks another task and retries that failed one later.



## Downloading Files

```python
def add_file_download(url: str,
                      labels_master: set = None,
                      labels_version: set = None,
                      force_new_version: bool = False):
```

Then you [created the bot](create-a-bot.md) you set two parameters:
* `filename_prefix`, and
* `target_directory`

So if you pass an URL to `add_file_download()` and start the processing, exoskeleton will download the file into the specified directory. The filename will consist of the chosen prefix and the alphanumeric UUID used in the database to identify the document version.

Sometimes the URL does not contain the appropriate file ending. This is very often the case if you process a homepage and have an URL like `https://www.example.com` instead of `https://www.example.com/index.html`. As this case is so common, exoskeleton analyzes the HTTP header send by the server. So if the server announces the mime type 'text/html', the file will be stored with the file ending `.html` and accordingly with other file types.


## Saving the Page Code

Sometimes the code of a page is relevant. With `add_save_page_code()` this will not be saved in a file, but *stored in the database*.

```python
def add_save_page_code(url: str,
                       labels_master: set = None,
                       labels_version: set = None,
                       prettify_html: bool = False,
                       force_new_version: bool = False):
```

The option `prettify_html` uses lxml to fix broken HTML code. *It should not be used with an XHTML or XML file.*

Take for example this broken input:
```html
<h1>Test with broken and incomplete HTML</h1>

<div>

<p>HTML Entities instead of unicode: &auml;&uuml;&ouml;</p>

<p><b>Paragraph and bold not closed

</div>

<div>

<p>New Paragraph</p>

</div>
```

If you save it with `prettify_html` set to True, the HTML is improved in a number of ways:

* a html, and a body tag are added
* HTML Entities are turned to unicode (`&uuml` into `ü`, ...)
* Open tags are closed
* the formatting is improved
* ...


```html
<html>
 <body>
  <h1>
   Test with broken and incomplete HTML
  </h1>
  <div>
   <p>
    HTML Entities instead of unicode: äüö
   </p>
   <p>
    <b>
     Paragraph and bold not closed
    </b>
   </p>
  </div>
  <div>
   <p>
    New Paragraph
   </p>
  </div>
 </body>
</html>
```


The function accepts sets of [labels](versions-and-labels.md "How to use labels").



## Saving a PDF version of a Web Site

```python
def add_page_to_pdf(url: str,
                    labels_master: set = None,
                    labels_version: set = None,
                    force_new_version: bool = False):
```

You can add a set of [labels](versions-and-labels.md "How to use labels").

This feature uses headless Chrome / Chromium to save a website as PDF. For it to work you need to set the path to the program.

**BEWARE**: *This function is not reliable for every website. Chrome / Chromium does not remember web cookies in headless mode. As a result, some websites black out the whole page and only display a cookie / data privacy dialogue and only this gets stored.*


> :arrow_right: **[Now learn how to steer the bot's behavior](behavior-settings.md)**