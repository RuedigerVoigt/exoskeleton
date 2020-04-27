# Exoskeleton Bot Behavior Settings

You can steer your bot's behavior in a number of ways via the `bot_behavior` parameter in the form of a dictionary when you [create it](create-a-bot.md "How to create a bot object").

## Available Settings and Default values

The available settings already have reasonable defaults:

| Parameter  | Default Value | Behavior |
| ------------- | ------------- | ------------- |
| **connection_timeout** | 60 seconds | Seconds until a connection times out. Without this the bot could freeze if a server does not reply at all. |
| **queue_max_retries** | 3 | **Not yet implemented**
| **queue_revisit** | 60 seconds | Seconds to wait until the queue is checked again if there are no tasks available. |
| **stop_if_queue_empty** | False | If set to `True` the bot stops once there are no actionable items in the queue left. That means there could be some with permanent errors, or some with seemingly temporary errors that exceeded the number of retries. Default is set to `False` so the bot continues to check the queue for new tasks until execution is aborted. |
| **wait_min** | 5 seconds | Minimum number of seconds to wait until the next task. |
| **wait_max** | 30 seconds | Maximum number of seconds to wait until the next task. |

The actual time between two tasks is a random number of seconds between `wait_min` and `wait_max`. If you download a high number of files or large files, you should set those parameters to high values in order to avoid load on the site serving those files. If you set the wait time very high, you should look up the `KeepAlive` time of your MariaDB instance. Typically, this is 60 seconds, so if exoskeleton has no activity for this time, it loses the database connection and has to reopen it.

## Example

```python
# -*- coding: utf-8 -*-
# File: bot.py

import logging

import exoskeleton
import credentials

logging.basicConfig(level=logging.DEBUG)


exo = exoskeleton.Exoskeleton(
    database_settings={'database': credentials.db,
                       'username': credentials.user,
                       'passphrase': credentials.passphrase},
    bot_behavior={'queue_max_retries': 5,
                  'wait_min': 20,
                  'wait_max': 50},
    filename_prefix='TEST_',
    target_directory='/home/yourusername/testfolder/'
)
```

> :arrow_right: **[Send progress reports by email](progress-reports-via-email.md)**