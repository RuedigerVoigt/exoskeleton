# File Versions and Labels

Exoskeleton supports managing multiple versions of the same file. It enables you to organize those files and versions with labels.


# Working with Versions

If you download a new file with exoskeleton the file itself will be stored in a folder, but the database will hold two entries: an entry in the table `fileMaster` and another one in `fileVersions` with the just downloaded file as version 1.

If you handle the same URL in a different way (like creating a pdf version after storing the page code) another version will be added and linked to the same `fileMaster` entry. If you perform the same task on the same URL, this will be blocked except you [tell exoskeleton to add a possible duplicate](avoiding-duplicates.md "handling of duplicates").

If you work with the file you can explicitly add new versions. Possible use cases:
* You downloaded images, but your machine learning algorithm needs them in 64x64. You keep the original, the downsized version and can generate a new version (128x128, ...) from the original in case you ever need it.
* You download a text and you store a tokenized version besides it.
* You downloaded a PDF, apply OCR and store the improved version.
* ...

## Adding Versions

Besides the automatic ways described above, you can always explicitly add a new version.


## Removing Versions

Once there are no versions of a file left, the entry in the `fileMaster` table is automatically removed by a database trigger.

# Organizing with Labels

If you download thousands of files organizing them becomes important. Labels help you with that. You can attach them to the main entry of a file and to each version of it. There is no limit to the number of labels you can attach.

Example:
* You want to download parts of an archive, which organizes its files by keyword.
* You parse the search results for your first keyword and add it as a label to the master entry. Later on you can find those files by the keyword as a label in the exoskeleton database.
* You parse the search results for the next keyword you are interested in. You also add this keyword as a label. Most likely some documents were already matches for the previous keyword. Exoskeleton will [detect those duplicates](avoiding-duplicates.md "automatic handling of duplicates") and will not add them to the queue. However, the new label is added to the existing entry.
* After you are done, you can easily find documents by keyword.

Other Use Cases:
* Add license information to a file.
* Add metadata like the author or creator of a file.
* Add labels that describe the state of a file like "needs OCR".

All [functions to handle files](handling-pages.md) allow you to add labels. For example:
```python
def add_file_download(url: str,
                      labels_master: set = None,
                      labels_version: set = None,
                      force_new_version: bool = False):
```

The parameter `labels_master` expects a set of labels which will be attached to the file's master entry. With the `labels_version` parameter you can attach labels to that specific version of the file. So, this could be an example call:

```python
add_file_download('https://www.example.com/example.pdf',
                  {'Keyword: Example', 'License: Creative Commons'},
                  {'needs OCR'},
                  False)
```
You do not need to add the file size as it is already available in the `fileVersions` table.

## Finding all files with a specific label

Use `version_uuids_by_label` to get a list of all file versions, i.e. UUIDs, with a specific label.

```python
def version_uuids_by_label(self,
                           single_label: str) -> list:
```

## Get all labels attached to a file

Use `version_labels_by_uuid` to get a list of all label names attached to a specific version of a file. This does not return labels attached to the entry in `fileMaster`.

```python
def version_labels_by_uuid(self,
                           version_uuid: str) -> list:
```

> :arrow_right: **[Learn about Data Sequences](data-sequences.md)**
