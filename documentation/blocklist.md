# Using the Blocklist

Exoskeleton automatically helps you to [avoid duplicates](avoiding-duplicates.md "How to avoid duplicates in exoskeleton"). The blocklist is a feature to exclude some pages to be processed at all.

The blocklist is based on the fully qualified domain name (FQDN). The FQDN for `https://www.example.com/foo.html?id=1` would be `www.example.com`.

If `www.example.com` is on the blocklist, exoskeleton will not add any URL to the queue whose FQDN matches that. If the URL is already in the queue, the task will not be executed if the FQDN has been added to the blocklist in the meantime. Exoskeleton checks again while processing the queue.

However `example.com` (missing 'www') is a different FQDN and `https://example.com/foo.html?id=1` (missing 'www') would be processed with `www.example.com` on the blocklist even if it has the same content. Be aware that some browsers blend out the 'www' until you click on the URL. Exoskeleton's blocklist does not support wildcards or regular expressions.

You have three functions to control the blocklist:
```python
def block_fqdn(self,
               fqdn: str,
               comment: Optional[str] = None):
```
Use `block_fqdn('www.example.com')` to add `www.example.com` to the blocklist. A comment is optional.

```python
def unblock_fqdn(self,
                 fqdn: str):
```

Expectedly `unblock_fqdn('www.example.com')` reverses `block_fqdn()` and removes `www.example.com` from the blocklist.

```python
def truncate_blocklist(self):
```
The command `truncate_blocklist()` removes all entries from the blocklist.


