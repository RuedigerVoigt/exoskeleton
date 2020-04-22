# File Versions and Labels

Exoskeleton supports managing multiple versions of the same file. It enables you to organize files and versions with labels.


# Working with Versions

If you download a file with exoskeleton the file itself will be stored in a folder, but the database will hold two entries: an entry in the table `fileMaster` and another one in `fileVersions` with the just downloaded file as version 1.

If you work with the file you can add new versions linked to the same entry in `fileMaster`. The same is true for a downloaded page code ([`add_save_page_code`](handling-pages.md)), and html pages stored as pdf ([`add_page_to_pdf`](handling-pages.md))

Possible use cases:
* You downloaded images, but your machine learning algorithm needs them in 64x64. You keep the original, the downsized version and can generate a new version (128x128, ...) from the original in case you ever need it.
* You download a text and you store a tokenized version besides it.
* ...

## Adding Versions

## Removing Versions

Once there is no versions of a file left, the entry in the `fileMaster` table is automatically removed by a database trigger.

# Organizing with Labels

You can attach labels to the main entry of the file and to each version of it. There is no limit to the number of labels you attach.