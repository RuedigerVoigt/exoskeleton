# Logging Configuration

Exoskeleton makes extensive use of Python's built-in logging functionality to provide visibility into its operations. This guide explains how to configure logging for your bot.

## Quick Start

The simplest way to enable logging is to add this to your bot script before creating the Exoskeleton instance:

```python
import logging

logging.basicConfig(level=logging.INFO)
```

## Understanding Log Levels

Python's logging module supports five standard log levels, from most to least verbose:

| Level | When to Use | What You'll See |
|-------|-------------|-----------------|
| **DEBUG** | Development and troubleshooting | Every detail: database queries, file operations, timer events, queue processing steps |
| **INFO** | Normal operation | Major events: files downloaded, database connections, queue status, configuration warnings |
| **WARNING** | Potential issues | Missing optional settings, fallback behaviors, unusual conditions |
| **ERROR** | Failures that need attention | HTTP errors, database problems, file I/O failures |
| **CRITICAL** | Severe failures | Not currently used by exoskeleton |

## Basic Configuration Examples

### Minimal Logging (Errors Only)

```python
import logging

logging.basicConfig(level=logging.ERROR)
```

### Normal Operation (INFO Level)

```python
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
```

### Detailed Debugging

```python
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
```

Notice the `%(name)s` format specifier - this shows which exoskeleton component generated each log message.

## Advanced Configuration

### Logging to a File

To save logs to a file instead of console output:

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='bot.log',
    filemode='a'  # 'a' for append, 'w' to overwrite each run
)
```

### Logging to Both File and Console

```python
import logging

# Create handlers
file_handler = logging.FileHandler('bot.log')
console_handler = logging.StreamHandler()

# Set levels
file_handler.setLevel(logging.DEBUG)  # Everything to file
console_handler.setLevel(logging.INFO)  # Only important stuff to console

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Get root logger and add handlers
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)
logger.addHandler(console_handler)
```

## Module-Level Logging Control

Exoskeleton uses hierarchical logger names like `exoskeleton.database_connection`, `exoskeleton.queue_manager`, etc. This allows you to control logging at a granular level.

### Example: Debug Database But Not Everything

```python
import logging

# Set default level to INFO
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Enable DEBUG only for database operations
logging.getLogger('exoskeleton.database_connection').setLevel(logging.DEBUG)
logging.getLogger('exoskeleton.database_schema_check').setLevel(logging.DEBUG)
```

### Example: Silence Verbose Components

```python
import logging

# Enable DEBUG globally
logging.basicConfig(level=logging.DEBUG)

# But silence the timer (very verbose)
logging.getLogger('exoskeleton.time_manager').setLevel(logging.WARNING)
```

### Available Logger Names

Exoskeleton provides these logger namespaces:

- `exoskeleton.core` - Main Exoskeleton class
- `exoskeleton.database_connection` - Database connectivity and sessions
- `exoskeleton.database_schema_check` - Schema validation
- `exoskeleton.queue_manager` - Queue processing logic
- `exoskeleton.actions` - Download and retrieval operations
- `exoskeleton.file_manager` - File I/O operations
- `exoskeleton.label_manager` - Label management
- `exoskeleton.error_manager` - Error handling and retries
- `exoskeleton.blocklist_manager` - Domain blocklisting
- `exoskeleton.job_manager` - Job tracking for multi-page crawls
- `exoskeleton.statistics_manager` - Download statistics
- `exoskeleton.notification_manager` - Email notifications
- `exoskeleton.time_manager` - Wait times and delays
- `exoskeleton.remote_control_chrome` - PDF generation with Chrome
- `exoskeleton.exo_url` - URL normalization and validation

## Understanding Exoskeleton's Log Output

### Database Connection Messages

When you start your bot, you'll see messages like:

```
INFO:exoskeleton.database_connection:No port number supplied: will try port 3306.
DEBUG:exoskeleton.database_connection:Trying to connect to database.
INFO:exoskeleton.database_connection:Successfully established database connection.
```

### Schema Validation

```
DEBUG:exoskeleton.database_schema_check:Checking if the database table structure is complete.
INFO:exoskeleton.database_schema_check:Found all expected tables.
INFO:exoskeleton.database_schema_check:Found all expected stored procedures.
```

### Queue Processing

```
DEBUG:exoskeleton.actions:starting download of queue id abc123...
INFO:exoskeleton.file_manager:Saving files in this directory: /path/to/files
DEBUG:exoskeleton.time_manager:5.3 seconds delay until next action
```

### Error Handling

```
ERROR:exoskeleton.actions:The bot hit a rate limit => increase min_wait.
INFO:exoskeleton.error_manager:Adding crawl delay to task abc123
WARNING:exoskeleton.database_connection:Lost database connection. Trying to reconnect...
INFO:exoskeleton.database_connection:Restored database connection!
```

## Best Practices

### 1. Use INFO for Production

For bots running in production, `INFO` level provides good visibility without overwhelming detail:

```python
logging.basicConfig(level=logging.INFO)
```

### 2. Use DEBUG for Development

When developing or troubleshooting, enable DEBUG to see everything:

```python
logging.basicConfig(level=logging.DEBUG)
```

### 3. Always Configure Before Creating Exoskeleton

Configure logging **before** instantiating the Exoskeleton class:

```python
# CORRECT ✓
import logging
import exoskeleton

logging.basicConfig(level=logging.INFO)
exo = exoskeleton.Exoskeleton(...)

# WRONG ✗ - Too late, some messages already logged
exo = exoskeleton.Exoskeleton(...)
logging.basicConfig(level=logging.INFO)
```

### 4. Include Timestamps in Production

For production bots that run for extended periods, always include timestamps:

```python
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
```

### 5. Rotate Log Files for Long-Running Bots

For bots that run for days or weeks, use rotating file handlers:

```python
from logging.handlers import RotatingFileHandler
import logging

handler = RotatingFileHandler(
    'bot.log',
    maxBytes=10*1024*1024,  # 10 MB
    backupCount=5  # Keep 5 old logs
)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)
```

## Common Scenarios

### Scenario 1: "I want to see what my bot is doing"

Use INFO level with timestamps:

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)
```

### Scenario 2: "My bot is failing and I need to debug it"

Use DEBUG level with full context:

```python
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

### Scenario 3: "Too many messages, I only want errors"

Use ERROR level:

```python
import logging

logging.basicConfig(level=logging.ERROR)
```

### Scenario 4: "I want detailed database logs but nothing else"

```python
import logging

logging.basicConfig(level=logging.WARNING)  # Only warnings/errors for most things

# But DEBUG for database
logging.getLogger('exoskeleton.database_connection').setLevel(logging.DEBUG)
logging.getLogger('exoskeleton.database_schema_check').setLevel(logging.DEBUG)
```

## Troubleshooting

### "I don't see any log messages"

Make sure you configured logging before creating the Exoskeleton instance, and check your log level isn't too restrictive:

```python
import logging

logging.basicConfig(level=logging.DEBUG)  # Most permissive
```

### "Too many log messages"

Increase the log level to INFO or WARNING:

```python
logging.basicConfig(level=logging.WARNING)
```

Or silence specific verbose components:

```python
logging.getLogger('exoskeleton.time_manager').setLevel(logging.ERROR)
```

### "I want different levels for different components"

Use module-level configuration as shown in the "Module-Level Logging Control" section above.

## Technical Background

Since version 3.0.0, exoskeleton uses module-level loggers following Python's logging best practices. Each module has a logger named after its Python module path (e.g., `exoskeleton.queue_manager`). This creates a hierarchical logging system that allows fine-grained control over log output.

This is a non-breaking change - your existing `logging.basicConfig()` calls will continue to work exactly as before.

---

> :arrow_right: **[Send progress reports by email](progress-reports-via-email.md)**
