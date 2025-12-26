# Test Database Safety Features

## Overview

Exoskeleton includes a safety system to prevent accidental execution of destructive tests against production databases. This document explains each safety mechanism and how to configure your test environment securely.

Running tests against a production database could result in:
- **Data loss** - Tests may truncate tables or delete records
- **Data corruption** - Tests insert test data that pollutes production
- **Service disruption** - Tests may lock tables or consume resources
- **Security breaches** - Test credentials may be less secure than production

The safety system is there to ensure these scenarios do not occur accidentally.

## The Safety Layers

### Layer 1: Separate Configuration File

**Purpose:** Keep test credentials isolated from production credentials.

**How it works:**
- Tests read ONLY from `.env.test`
- `.env.test` is for test databases only
- `.env` can contain production credentials safely


Both files should be in `.gitignore` to avoid being committed!

**Configuration:**
```bash
# .env.test - Test database credentials
EXOSKELETON_TEST_MODE=true
TEST_DB_HOST=localhost
TEST_DB_PORT=3306
TEST_DB_NAME=exoskeleton_test  # MUST contain 'test'
TEST_DB_USER=exoskeleton_test
TEST_DB_PASSWORD=test_password
```

### Layer 2: Database Name Validation

**Purpose:** Enforce that test databases have 'test' in their name.

**How it works:**
- Database name is validated before ANY database connection
- Name must contain the string 'test' (case-insensitive)
- Tests abort with clear error if validation fails

**Valid database names:**
- `exoskeleton_test` ✓
- `test_exoskeleton` ✓
- `my_test_db` ✓
- `testing_database` ✓

**Invalid database names:**
- `exoskeleton` ✗
- `production` ✗
- `exo_prod` ✗


### Layer 3: Data Quantity Check

**Purpose:** Detect if a database contains production data before running tests.

**How it works:**
- Before test execution, queries `fileMaster` table for row count
- If count exceeds 100 entries, tests abort
- Assumes production databases have substantial data
- Assumes test databases are empty or lightly populated

**Threshold:** 100 entries in `fileMaster` table (configurable)

**Error message:**
```
======================================================================
SAFETY CHECK FAILED: Database has 5000 entries in fileMaster
======================================================================
This looks like production data. Tests should use an empty database.
Create a fresh test database or truncate all tables first.
======================================================================
```

**Bypass:** This check is skipped in CI/CD environments (when `CI` env var is set) because service containers are always fresh.


### Layer 4: TEST_* Environment Variable Prefix

**Purpose:** Make test credentials immediately recognizable as test-only.

**How it works:**
- All test environment variables use `TEST_*` prefix
- Makes intent obvious in configuration files
- Reduces confusion between test and production variables

**Pattern:**
```bash
# Test variables (for testing)
TEST_DB_HOST=localhost
TEST_DB_NAME=exoskeleton_test
TEST_DB_USER=test_user

# Production variables (for application)
DB_HOST=prod-server.example.com
DB_NAME=exoskeleton_production
DB_USER=prod_user
```

**What it prevents:**
- Copy-paste errors between test and production config
- Confusion about which credentials are for what purpose

### Layer 5: Explicit Test Mode Flag

**Purpose:** Require conscious decision to enable test execution.

**How it works:**
- Must set `EXOSKELETON_TEST_MODE=true` in `.env.test`
- Tests check this flag before initializing
- Flag must be explicitly set to the string `"true"`
- Any other value (including absence) causes test abort

**Configuration:**
```bash
# In .env.test
EXOSKELETON_TEST_MODE=true  # Required!
```



**What it prevents:**
- Tests running accidentally when developer forgets they have `.env.test`
- Automated scripts inadvertently triggering test suite


## Quick Start: Secure Test Setup

### Step 1: Create Test Database

**Option A: Docker (Recommended)**
```bash
docker run -d \
  --name exoskeleton-test \
  -e MYSQL_ROOT_PASSWORD=root \
  -e MYSQL_DATABASE=exoskeleton_test \
  -e MYSQL_USER=exoskeleton_test \
  -e MYSQL_PASSWORD=test_password \
  -p 3306:3306 \
  mariadb:latest

# Load schema
docker exec -i exoskeleton-test mysql -uexoskeleton_test -ptest_password exoskeleton_test < Database-Scripts/Generate-Database-Schema-MariaDB.sql
```

**Option B: Local MariaDB**
```bash
mysql -u root -p
CREATE DATABASE exoskeleton_test CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'exoskeleton_test'@'localhost' IDENTIFIED BY 'test_password';
GRANT ALL PRIVILEGES ON exoskeleton_test.* TO 'exoskeleton_test'@'localhost';
FLUSH PRIVILEGES;

mysql -u exoskeleton_test -p exoskeleton_test < Database-Scripts/Generate-Database-Schema-MariaDB.sql
```

### Step 2: Configure .env.test

```bash
# Copy example file
cp .env.test.example .env.test

# Edit .env.test with your test database credentials
# IMPORTANT: Database name MUST contain 'test'
```

Example `.env.test`:
```bash
EXOSKELETON_TEST_MODE=true
TEST_DB_HOST=localhost
TEST_DB_PORT=3306
TEST_DB_NAME=exoskeleton_test
TEST_DB_USER=exoskeleton_test
TEST_DB_PASSWORD=test_password
TEST_BROWSER=chromium-browser
```

### Step 3: Run Tests

```bash
# Run all tests
python -m pytest

# Or run specific test suites
python -m pytest tests/tests_without_side_effects.py  # Unit tests (no DB)
python -m pytest tests/tests_with_side_effects.py      # System tests (requires DB)
```

## CI/CD Behavior

In Continuous Integration environments (GitHub Actions, GitLab CI, etc.):
- Layer 1: Uses environment variables (no `.env.test` file needed)
- Layer 2: Database name validated (CI uses `exoskeleton_test`)
- Layer 3: **Bypassed** (CI service containers are always fresh/empty)
- Layer 4: Uses `TEST_*` prefixed variables
- Layer 5: **Bypassed** (CI environment is safe by design)

The `CI` environment variable (automatically set by CI platforms) signals that safety checks 3 and 5 can be relaxed.

## Troubleshooting

### Tests won't run: "EXOSKELETON_TEST_MODE not set to 'true'"

**Solution:** Add to `.env.test`:
```bash
EXOSKELETON_TEST_MODE=true
```

### Tests won't run: "Database name must contain 'test'"

**Solution:** Rename your database to include 'test':
```sql
-- Rename database
CREATE DATABASE exoskeleton_test;
-- Copy data from old database if needed
-- Update .env.test with new name
```

### Tests won't run: "Database has 5000 entries in fileMaster"

**Solution:** You're using a populated database. Either:
1. Create a fresh empty test database (recommended)
2. Truncate all tables: `CALL truncate_all_tables_SP();` (if such procedure exists)
3. Use a different database for testing

### Want to bypass safety checks (NOT RECOMMENDED)

**Don't do this in production or with real data!**

The only legitimate reason to bypass is testing the test suite itself. If you absolutely must:

1. Temporarily comment out validation in `tests/tests_with_side_effects.py`
2. Understand you're removing critical safety features
3. Restore safety checks immediately after

## Best Practices


### ❌ DON'T:
- Don't put production credentials in `.env.test`
- Don't name test databases ambiguously (e.g., `exoskeleton`, `main`)
- Don't run tests against databases with production data
- Don't commit `.env.test` to version control
- Don't disable safety checks in production environments

## See Also

- [Installation Guide](installation.md) - Initial setup and database creation
- [Logging Configuration](logging-configuration.md) - Configure logging for tests
- [Testing Exoskeleton](testing-exoskeleton.md) - Running the test suite
- [Create a Bot](create-a-bot.md) - Using exoskeleton in your application
