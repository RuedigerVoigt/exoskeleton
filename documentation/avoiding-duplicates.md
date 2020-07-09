# Avoiding Duplicates on the Fly

If you [parse search results](parse-search-results.md), it is likely that some documents show up for multiple keywords. This might lead to you downloading the same files multiple times. You want to avoid this as it slows you down. Furthermore, it puts unnecessary stress on the server.

**For this reason, exoskeleton detects duplicates automatically.** If you try to handle a file which is in the queue or already has been handled, exoskeleton will inform you with a logging message and will ignore the request. However, if you added new [labels](versions-and-labels.md) to the main entry, those will be added to the existing entry.

**There are two exceptions:**:
* If you perform different tasks on the same file (like downloading its source code *and* storing a PDF version), then each task will create a version entry under the same entry in the `fileMaster` table.
* You can force exoskeleton with setting the `force_new_version` parameter to `True` to create another [version](versions-and-labels.md "File versions and labels") under the same master entry. This is especially useful if you want to build a history of a page. An example would be a news front page which you want to store daily to analyze topics in the log run.

> :arrow_right: **[Now learn about the options for storing pages and files](handling-pages.md)**
