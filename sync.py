# MSSQL GitLab Sync
# Copyright (c) 2025 emilcube
# Licensed under the MIT License

import re
import pyodbc
import os
import logging
import gitlab
from datetime import datetime
from typing import Tuple
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sp_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

SQL_HOST = os.getenv('SQL_HOST')
SQL_USER = os.getenv('SQL_USER')
SQL_PASSWORD = os.getenv('SQL_PASSWORD')

# Database selection mode: 'specific', 'all', 'all_except'
DATABASE_MODE = os.getenv('DATABASE_MODE', 'specific')
DATABASES = os.getenv('DATABASES', 'YOUR_DB').split(',') if os.getenv('DATABASES') else ['YOUR_DB']
EXCLUDE_DATABASES = os.getenv('EXCLUDE_DATABASES', 'master,tempdb,model,msdb').split(',')

GITLAB_URL = os.getenv('GITLAB_URL')
GITLAB_TOKEN = os.getenv('GITLAB_TOKEN')
GITLAB_PROJECT = os.getenv('GITLAB_PROJECT')
GITLAB_BRANCH = os.getenv('GITLAB_BRANCH')

# Object type mapping for folder organization
TYPE_FOLDERS = {
    'V':  'views',       # View
    'P':  'procedures',  # Stored Procedure
    'FN': 'others',  # Scalar Function
    'IF': 'others',  # Inline Table-Valued Function
    'TF': 'others',  # Multi-statement Table-Valued Function
    'TR': 'others',  # Trigger
}


# ---- SQL QUERY ----
QUERY = """
SELECT 
    s.name AS schema_name,
    o.name AS object_name,
    o.type AS object_type,
    m.definition
FROM sys.objects o
JOIN sys.sql_modules m ON o.object_id = m.object_id
JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE o.type IN ('P','FN','IF','TF','V','TR')
ORDER BY s.name, o.name
"""

def connect_to_server():
    """Establish server connection."""
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SQL_HOST};"
            f"UID={SQL_USER};"
            f"PWD={SQL_PASSWORD};",
            timeout=10
        )
        logger.info(f"Connected to {SQL_HOST}")
        return conn
    except pyodbc.Error as e:
        logger.error(f"Server connection failed: {e}")
        raise

def get_databases(cursor) -> list:
    """Get list of databases based on configuration mode."""
    if DATABASE_MODE == 'specific':
        return DATABASES
    
    # Get all databases from server
    cursor.execute("""
        SELECT name 
        FROM sys.databases 
        WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
        AND state_desc = 'ONLINE'
        ORDER BY name
    """)
    all_dbs = [row[0] for row in cursor.fetchall()]
    
    if DATABASE_MODE == 'all':
        logger.info(f"Mode 'all': Found {len(all_dbs)} databases")
        return all_dbs
    elif DATABASE_MODE == 'all_except':
        filtered_dbs = [db for db in all_dbs if db not in EXCLUDE_DATABASES]
        logger.info(f"Mode 'all_except': {len(filtered_dbs)} databases (excluded {len(all_dbs) - len(filtered_dbs)})")
        return filtered_dbs
    else:
        logger.error(f"Unknown DATABASE_MODE: {DATABASE_MODE}")
        raise ValueError(f"Invalid DATABASE_MODE: {DATABASE_MODE}")

def connect_to_database(database_name):
    """Establish database connection with error handling."""
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SQL_HOST};"
            f"DATABASE={database_name};"
            f"UID={SQL_USER};"
            f"PWD={SQL_PASSWORD};",
            timeout=10
        )
        logger.info(f"Connected to {database_name} on {SQL_HOST}")
        return conn
    except pyodbc.Error as e:
        logger.error(f"Database connection failed for {database_name}: {e}")
        raise

# def fetch_database_objects(cursor) -> list:
#     """Fetch all stored procedures, functions, views, and triggers."""
#     try:
#         cursor.execute(QUERY)
#         rows = cursor.fetchall()
#         logger.info(f"Retrieved {len(rows)} database objects")
#         return rows
#     except pyodbc.Error as e:
#         logger.error(f"Failed to fetch database objects: {e}")
#         raise

def fetch_database_objects(cursor) -> list:
    """Fetch all stored procedures, functions, views, and triggers."""
    try:
        cursor.execute(QUERY)
        rows = cursor.fetchall()
        
        # Convert CREATE to CREATE OR ALTER for procedures/functions
        modified_rows = []
        for schema_name, object_name, object_type, definition in rows:
            if definition:
                obj_type = object_type.strip()
                # For stored procedures and functions, replace CREATE with CREATE OR ALTER
                if obj_type in ('P', 'FN', 'IF', 'TF'):
                    if obj_type == 'P':
                        # Handle both PROCEDURE and PROC
                        definition = re.sub(r'\bCREATE\s+PROCEDURE\b', 'CREATE OR ALTER PROCEDURE', definition, flags=re.IGNORECASE, count=1)
                        definition = re.sub(r'\bCREATE\s+PROC\b', 'CREATE OR ALTER PROC', definition, flags=re.IGNORECASE, count=1)
                    elif obj_type in ('FN', 'IF', 'TF'):
                        definition = re.sub(r'\bCREATE\s+FUNCTION\b', 'CREATE OR ALTER FUNCTION', definition, flags=re.IGNORECASE, count=1)
                # For views, use CREATE OR ALTER VIEW
                elif obj_type == 'V':
                    definition = re.sub(r'\bCREATE\s+VIEW\b', 'CREATE OR ALTER VIEW', definition, flags=re.IGNORECASE, count=1)
                # For triggers, use CREATE OR ALTER TRIGGER
                elif obj_type == 'TR':
                    definition = re.sub(r'\bCREATE\s+TRIGGER\b', 'CREATE OR ALTER TRIGGER', definition, flags=re.IGNORECASE, count=1)
            
            modified_rows.append((schema_name, object_name, object_type, definition))
        
        logger.info(f"Retrieved {len(modified_rows)} database objects (converted to CREATE OR ALTER)")
        return modified_rows
    except pyodbc.Error as e:
        logger.error(f"Failed to fetch database objects: {e}")
        raise

def fetch_table_ddl(cursor) -> list:
    """Fetch DDL for all tables with indexes."""
    ddl_query = """
    SELECT 
        s.name AS schema_name,
        t.name AS table_name,
        'CREATE TABLE [' + s.name + '].[' + t.name + '] (' +
        STUFF((
            SELECT ',' + CHAR(13) + CHAR(10) + 
                '  [' + c.name + '] ' + 
                TYPE_NAME(c.user_type_id) +
                CASE 
                    WHEN TYPE_NAME(c.user_type_id) IN ('varchar', 'char', 'nvarchar', 'nchar') 
                    THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(
                        CASE WHEN TYPE_NAME(c.user_type_id) IN ('nvarchar', 'nchar') 
                        THEN c.max_length/2 ELSE c.max_length END AS VARCHAR) END + ')'
                    WHEN TYPE_NAME(c.user_type_id) IN ('decimal', 'numeric')
                    THEN '(' + CAST(c.precision AS VARCHAR) + ',' + CAST(c.scale AS VARCHAR) + ')'
                    ELSE ''
                END +
                CASE WHEN c.is_nullable = 0 THEN ' NOT NULL' ELSE ' NULL' END +
                CASE WHEN c.is_identity = 1 THEN ' IDENTITY(' + CAST(IDENT_SEED(s.name + '.' + t.name) AS VARCHAR) + ',' + CAST(IDENT_INCR(s.name + '.' + t.name) AS VARCHAR) + ')' ELSE '' END
            FROM sys.columns c
            WHERE c.object_id = t.object_id
            ORDER BY c.column_id
            FOR XML PATH(''), TYPE
        ).value('.', 'NVARCHAR(MAX)'), 1, 1, '') + CHAR(13) + CHAR(10) + ');' +
        -- Add indexes
        ISNULL((
            SELECT CHAR(13) + CHAR(10) + 
                CASE 
                    WHEN i.is_primary_key = 1 THEN 'ALTER TABLE [' + s.name + '].[' + t.name + '] ADD CONSTRAINT [' + i.name + '] PRIMARY KEY'
                    WHEN i.is_unique = 1 THEN 'CREATE UNIQUE INDEX [' + i.name + '] ON [' + s.name + '].[' + t.name + ']'
                    ELSE 'CREATE INDEX [' + i.name + '] ON [' + s.name + '].[' + t.name + ']'
                END +
                CASE WHEN i.type_desc = 'CLUSTERED' THEN ' CLUSTERED' ELSE '' END +
                ' (' +
                STUFF((
                    SELECT ',' + CHAR(13) + CHAR(10) + '  [' + COL_NAME(ic.object_id, ic.column_id) + ']' + 
                        CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
                    FROM sys.index_columns ic
                    WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
                    ORDER BY ic.key_ordinal
                    FOR XML PATH(''), TYPE
                ).value('.', 'NVARCHAR(MAX)'), 1, 1, '') + CHAR(13) + CHAR(10) + ');'
            FROM sys.indexes i
            WHERE i.object_id = t.object_id 
                AND i.type IN (1,2)
                AND i.is_primary_key = 0
            FOR XML PATH(''), TYPE
        ).value('.', 'NVARCHAR(MAX)'), '') AS ddl
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.is_ms_shipped = 0
    ORDER BY s.name, t.name
    """
    try:
        cursor.execute(ddl_query)
        rows = cursor.fetchall()
        logger.info(f"Retrieved {len(rows)} table DDL statements with indexes")
        return rows
    except pyodbc.Error as e:
        logger.error(f"Failed to fetch table DDL: {e}")
        raise

def connect_gitlab():
    """Connect to GitLab and get project."""
    try:
        gl = gitlab.Gitlab(GITLAB_URL, private_token=GITLAB_TOKEN)
        gl.auth()
        logger.info(f"Authenticated to GitLab as: {gl.user.username}")
        
        project = gl.projects.get(GITLAB_PROJECT)
        logger.info(f"Connected to project: {project.name}")
        return gl, project
    except gitlab.exceptions.GitlabAuthenticationError as e:
        logger.error(f"GitLab authentication failed: {e}")
        raise
    except gitlab.exceptions.GitlabGetError as e:
        logger.error(f"Failed to get project: {e}")
        raise

def get_existing_files(project, branch: str) -> set:
    """Get all existing SQL files from GitLab repository."""
    existing_files = set()
    
    try:
        items = project.repository_tree(ref=branch, recursive=True, all=True)
        
        for item in items:
            if item['type'] == 'blob' and item['path'].endswith('.sql'):
                existing_files.add(item['path'])
        
        logger.info(f"Found {len(existing_files)} existing SQL files in GitLab")
        return existing_files
        
    except gitlab.exceptions.GitlabGetError as e:
        logger.warning(f"Could not fetch repository tree: {e}")
        return existing_files

#def prepare_commit_actions(proc_rows: list, table_rows: list, existing_files: set) -> Tuple[list, int, int]:
def prepare_commit_actions(database_name: str, proc_rows: list, table_rows: list, existing_files: set) -> Tuple[list, int, int]:
    """Prepare GitLab commit actions - just update/create everything, let GitLab handle the rest."""
    actions = []
    current_files = set()
    
    # Process procedures/views/functions
    logger.info(f"[{database_name}] Preparing procedures/views/functions...")
    for schema_name, object_name, object_type, definition in proc_rows:
        type_folder = TYPE_FOLDERS.get(object_type.strip(), 'procedures')
        file_path = f"{database_name}/{type_folder}/{schema_name}.{object_name}.sql"
        current_files.add(file_path)
        
        content = definition or ""
        
        # Just update if exists, create if not - GitLab will skip if no change
        action = 'update' if file_path in existing_files else 'create'
        actions.append({
            'action': action,
            'file_path': file_path,
            'content': content
        })
    
    # Process tables
    logger.info(f"[{database_name}] Preparing tables...")
    for schema_name, table_name, ddl in table_rows:
        file_path = f"{database_name}/tables/{schema_name}.{table_name}.sql"
        current_files.add(file_path)
        
        content = ddl or ""
        
        action = 'update' if file_path in existing_files else 'create'
        actions.append({
            'action': action,
            'file_path': file_path,
            'content': content
        })
    
    # Only delete files from THIS database folder
    db_existing_files = {f for f in existing_files if f.startswith(f"{database_name}/")}
    deleted_files = db_existing_files - current_files

    for file_path in deleted_files:
        actions.append({
            'action': 'delete',
            'file_path': file_path
        })
    
    total_objects = len(proc_rows) + len(table_rows)
    deleted_count = len(deleted_files)
    
    logger.info(f"[{database_name}] Prepared {total_objects} objects and {deleted_count} deletions")
    return actions, total_objects, deleted_count

def commit_to_gitlab(project, actions: list, branch: str, commit_message: str):
    """Commit all changes to GitLab in a single commit."""
    if not actions:
        logger.info("No changes to commit")
        return
    
    try:
        data = {
            'branch': branch,
            'commit_message': commit_message,
            'actions': actions
        }
    
        commit = project.commits.create(data)
        
        if commit.stats['additions'] == 0 and commit.stats['deletions'] == 0:
            logger.info("No actual changes in this commit (GitLab skipped unchanged files)")
            return
    
        logger.info(f"Successfully committed {len(actions)} actions")
        logger.info(f"Commit ID: {commit.id}")
        return commit
        
    except gitlab.exceptions.GitlabCreateError as e:
        logger.error(f"Failed to create commit: {e}")
        raise

def main():
    """Main execution function."""
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("Starting SQL Server to GitLab sync")
    logger.info(f"Mode: {DATABASE_MODE}")
    logger.info("=" * 60)
    
    conn = None
    try:
        # 1. Connect to database
        conn = connect_to_server()
        cursor = conn.cursor()
        
        
        databases = get_databases(cursor)
        logger.info(f"Processing {len(databases)} database(s): {', '.join(databases)}")
        conn.close()

        # 2. Connect to GitLab
        gl, project = connect_gitlab()

        # 3. Get existing files from GitLab
        logger.info("Fetching existing files from GitLab...")
        existing_files = get_existing_files(project, GITLAB_BRANCH)
        
        # 4. Process each database
        all_actions = []
        db_stats = {}
        
        for database_name in databases:
            logger.info("=" * 60)
            logger.info(f"Processing database: {database_name}")
            logger.info("=" * 60)
            
            try:
                # Connect to specific database
                db_conn = connect_to_database(database_name)
                db_cursor = db_conn.cursor()
                
                # Fetch objects
                proc_rows = fetch_database_objects(db_cursor)
                table_rows = fetch_table_ddl(db_cursor)
                
                # Prepare actions for this database
                actions, total_objects, deleted_count = prepare_commit_actions(
                    database_name, proc_rows, table_rows, existing_files
                )
                
                all_actions.extend(actions)
                db_stats[database_name] = {
                    'objects': total_objects,
                    'deletions': deleted_count
                }
                
                db_conn.close()
                logger.info(f"[{database_name}] Completed: {total_objects} objects, {deleted_count} deletions")
                
            except Exception as e:
                logger.error(f"[{database_name}] Failed: {e}")
                # Continue with other databases
                continue
        
        #  5. Commit everything to GitLab
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        commit_msg = f"mssql sync: {timestamp}"

        total_objects = sum(s['objects'] for s in db_stats.values())
        total_deletions = sum(s['deletions'] for s in db_stats.values())

        logger.info(f"Committing to GitLab...")
        commit_to_gitlab(project, all_actions, GITLAB_BRANCH, commit_msg)
        
        # 6. Summary
        duration = (datetime.now() - start_time).total_seconds()
        logger.info("=" * 60)
        logger.info("Sync completed successfully!")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Databases processed: {len(db_stats)}")
        logger.info(f"Total objects: {total_objects}")
        logger.info(f"Deletions: {total_deletions}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Sync failed: {e}", exc_info=True)
        raise
    
if __name__ == "__main__":
    main()