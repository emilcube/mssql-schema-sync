# mssql schema sync

Automated synchronization of SQL Server database schemas to GitLab repository. Exports stored procedures, functions, views, and table DDLs with indexes to version-controlled SQL files

## Table of сontents

- [Features](#features)
- [Why use this](#why-use-this)
- [How it works](#how-it-works)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Database selection modes](#database-selection-modes)
- [Usage](#usage)
- [Repository structure](#repository-structure)
- [Output format](#output-format)
- [Logging](#logging)
- [Scheduling](#scheduling)
- [GitLab token permissions](#gitlab-token-permissions)
- [License](#license)

## Features

- Export table DDL with CREATE TABLE statements including columns, data types, and indexes
- Export stored procedures, functions, views, and triggers with CREATE OR ALTER syntax
- Support for multiple databases with flexible selection modes
- Automatic detection and deletion of removed database objects
- Single commit per sync with detailed statistics
- Organized file structure by database and object type

## Why use this

- Track database schema changes over time through periodic snapshots
- Review modifications to stored procedures, functions, views, and tables between sync runs
- Recover deleted or modified database objects from Git history
- Compare database schemas across different time periods
- Audit database modifications and maintain compliance requirements
- Collaborate on database changes with team code review workflows

Note: This tool creates snapshots at each sync interval. It does not capture real-time changes, only differences between scheduled runs.

## How it works

The sync process follows these steps:

1. Connects to SQL Server and retrieves all stored procedures, functions, views, triggers, and table DDLs
2. Retrieves the list of existing SQL files from GitLab repository (paths only, not content)
3. Compares lists to identify files that were deleted from the database
4. Sends all database objects and deletions to GitLab in a single commit
5. GitLab automatically skips files that haven't changed

The script uses the GitLab API directly, without git commands or local repository clones. No content comparison is done by the script - GitLab handles this automatically when processing the commit.

## Requirements

- Python 3.10+
- SQL Server with ODBC Driver 17
- GitLab account with repository access
- Poetry for dependency management

## Installation
```bash
git clone https://github.com/emilcube/mssql-schema.git
cd mssql-schema
poetry install
```

## Configuration

Create `.env` file in project root:
```env
# SQL Server connection
SQL_HOST=your_server_address
SQL_USER=your_username
SQL_PASSWORD=your_password

# Database selection mode: 'specific', 'all', 'all_except'
DATABASE_MODE=specific
DATABASES=Database1,Database2

# For 'all_except' mode (optional)
EXCLUDE_DATABASES=TestDB,TempDB

# GitLab configuration
GITLAB_URL=https://gitlab.com
GITLAB_TOKEN=your_gitlab_token
GITLAB_PROJECT=namespace/project-name
GITLAB_BRANCH=main
```

## Database selection modes

### Specific databases
```env
DATABASE_MODE=specific
DATABASES=ProductionDB,AnalyticsDB
```

### All databases (excludes system databases automatically)
```env
DATABASE_MODE=all
```

### All except specified
```env
DATABASE_MODE=all_except
EXCLUDE_DATABASES=TestDB,OldDB,ArchiveDB
```

## Usage
```bash
poetry run python sync.py
```

## Repository structure

Files are organized by database and object type:
```
DatabaseName/
├── procedures/
│   └── dbo.StoredProcedureName.sql
├── others/
│   ├── dbo.FunctionName.sql
│   └── dbo.TriggerName.sql
├── tables/
│   └── dbo.TableName.sql
└── views/
    └── dbo.ViewName.sql
```

## Output format

**Stored Procedures/Functions:**
- Uses CREATE OR ALTER syntax for idempotent execution

**Tables:**
- Complete CREATE TABLE statement
- All columns with data types, nullability, and identity properties
- CREATE INDEX statements for all non-primary key indexes

## Logging

Logs are written to `sp_sync.log` and console output. Each run includes:
- Connection status
- Objects processed per database
- Commit statistics
- Total execution time

## Scheduling

The script can be scheduled using Apache Airflow or other orchestration tools. There is a DAG example file: mssql_schema_sync_dag.py, which can be used to automate the sync process in Airflow.
Or just simple add to crontab for automated daily sync:
```bash
0 2 * * * cd /path/to/project && poetry run python sync.py
```

## GitLab token permissions

Required token scopes:
- `api`
- `write_repository`

## License

MIT