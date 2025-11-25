# --- utils/db_utils.py ---

import pyodbc
import logging # Assuming logging is configured in main script
from utils.common import log_print # Assuming log_print is in common.py
from datetime import datetime # For Python-side timestamping if needed

def connect_to_db(server, port, username, password, database=None, autocommit=False, timeout=5):
    """Establishes a connection to the SQL Server database."""
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};" # Or other appropriate driver like {{SQL Server Native Client 10.0}} for older systems
            f"SERVER={server},{port};"
            f"UID={username};PWD={password};"
            f"TrustServerCertificate=yes;"
        )
        if database:
            conn_str += f"DATABASE={database};"
        
        conn = pyodbc.connect(conn_str, timeout=timeout, autocommit=autocommit)
        log_print(f"Successfully connected to {server}:{port}/{database or 'master'}", level="info")
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        log_print(f"Database connection error to {server}:{port}/{database or 'master'}. SQLSTATE: {sqlstate}. Error: {ex}", level="error")
        raise


def ensure_database_exists(master_conn_details, db_name):
    """
    Ensures a database exists. Connects to 'master' to create it if not present.
    master_conn_details should be a dict with server, port, username, password.
    """
    conn = None
    try:
        log_print(f"Ensuring database '{db_name}' exists on {master_conn_details['server']}.", level="info")
        conn = connect_to_db(
            server=master_conn_details['server'],
            port=master_conn_details['port'],
            username=master_conn_details['username'],
            password=master_conn_details['password'],
            database='master',
            autocommit=True 
        )
        cur = conn.cursor()
        cur.execute("SELECT name FROM sys.databases WHERE name = ?", (db_name,))
        if cur.fetchone():
            log_print(f"Database '{db_name}' already exists.", level="info")
        else:
            log_print(f"Database '{db_name}' does not exist. Attempting to create.", level="info")
            safe_db_name = db_name.replace("'", "''").replace("]", "]]") 
            create_db_sql = f"CREATE DATABASE [{safe_db_name}]"
            cur.execute(create_db_sql)
            log_print(f"Database '{db_name}' created successfully.", level="success")
        cur.close()
    except pyodbc.Error as e:
        log_print(f"Error ensuring database '{db_name}' exists: {e}", level="error")
        raise
    finally:
        if conn:
            conn.close()


def ensure_sync_schema_and_meta(db_conn: pyodbc.Connection): # Expects a connection
    """
    Ensures the 'sync' schema and 'SyncMeta' table (with new status columns) exist.
    This function manages its own commit for DDL changes to SyncMeta.
    Uses DATETIME for SQL Server 2008 compatibility for new timestamp columns.
    """
    create_sync_meta_sql = """
    IF NOT EXISTS (
        SELECT 1
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'sync' AND TABLE_NAME = 'SyncMeta'
    )
    BEGIN
        CREATE TABLE [sync].[SyncMeta] (
            BranchName NVARCHAR(255) NOT NULL,
            TableName  NVARCHAR(255) NOT NULL,
            LastValue  NVARCHAR(255) NOT NULL,
            LastSynced DATETIME DEFAULT GETDATE(), -- Using DATETIME for SQL 2008 compat
            SyncStatus NVARCHAR(20) DEFAULT 'Pending' NOT NULL,
            LastCompletionTime DATETIME NULL,      -- Using DATETIME for SQL 2008 compat
            SyncRemarks NVARCHAR(MAX) NULL,                    
            CONSTRAINT PK_SyncMeta PRIMARY KEY (BranchName, TableName)
        );
        CREATE INDEX IX_SyncMeta_LastSynced ON [sync].[SyncMeta](LastSynced);
        CREATE INDEX IX_SyncMeta_SyncStatus ON [sync].[SyncMeta](SyncStatus);
        PRINT 'SyncMeta table created with new status columns (DATETIME for compatibility).';
    END
    ELSE
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'sync' AND TABLE_NAME = 'SyncMeta' AND COLUMN_NAME = 'SyncStatus')
        BEGIN
            ALTER TABLE [sync].[SyncMeta] ADD SyncStatus NVARCHAR(20) DEFAULT 'Pending' NOT NULL;
            PRINT 'Added SyncStatus column to SyncMeta.';
        END
        IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'sync' AND TABLE_NAME = 'SyncMeta' AND COLUMN_NAME = 'LastCompletionTime')
        BEGIN
            ALTER TABLE [sync].[SyncMeta] ADD LastCompletionTime DATETIME NULL; -- Using DATETIME
            PRINT 'Added LastCompletionTime column to SyncMeta.';
        END
        IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'sync' AND TABLE_NAME = 'SyncMeta' AND COLUMN_NAME = 'SyncRemarks')
        BEGIN
            ALTER TABLE [sync].[SyncMeta] ADD SyncRemarks NVARCHAR(MAX) NULL;
            PRINT 'Added SyncRemarks column to SyncMeta.';
        END
        -- Note: Altering existing LastSynced from DATETIME2 to DATETIME if needed is complex and not handled here.
        -- This script ensures new creations use DATETIME and adds new columns as DATETIME.
    END
    """
    try:
        with db_conn.cursor() as cur:
            cur.execute("IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'sync') BEGIN EXEC('CREATE SCHEMA [sync]') END")
            log_print("Ensured 'sync' schema exists in target.", level="debug")
            
            cur.execute(create_sync_meta_sql)
            log_print("Ensured 'sync.SyncMeta' table structure is up-to-date with status columns.", level="debug")
        db_conn.commit() 
    except pyodbc.Error as e:
        log_print(f"Error ensuring sync schema/meta table structure: {e}", level="error")
        db_conn.rollback()
        raise

def get_sync_meta_entry(cursor: pyodbc.Cursor, branch_name: str, table_name: str, create_if_not_exists: bool = True) -> dict | None:
    """
    Gets the full SyncMeta entry for a table and branch.
    If create_if_not_exists is True, it creates a 'Pending' entry if one doesn't exist.
    This function does NOT commit; relies on calling context for transaction management if create_if_not_exists is True.
    """
    default_last_value = '0'
    default_status = 'Pending'
    
    try:
        cursor.execute(
            "SELECT LastValue, LastSynced, SyncStatus, LastCompletionTime, SyncRemarks "
            "FROM [sync].[SyncMeta] WHERE BranchName = ? AND TableName = ?",
            (branch_name, table_name)
        )
        row = cursor.fetchone()
        if row:
            return {
                "LastValue": str(row[0]), "LastSynced": row[1], "SyncStatus": row[2],
                "LastCompletionTime": row[3], "SyncRemarks": row[4]
            }
        elif create_if_not_exists:
            log_print(f"No SyncMeta entry for {branch_name}:{table_name}. Creating with status '{default_status}'.", level="info")
            # Use GETDATE() for LastSynced on initial insert
            cursor.execute(
                "INSERT INTO [sync].[SyncMeta] (BranchName, TableName, LastValue, SyncStatus, LastSynced) VALUES (?, ?, ?, ?, GETDATE())",
                (branch_name, table_name, default_last_value, default_status)
            )
            # NO COMMIT HERE - calling function must commit this insert as part of its transaction
            return {
                "LastValue": default_last_value, "LastSynced": None, # Will be set by GETDATE() in DB
                "SyncStatus": default_status, "LastCompletionTime": None, "SyncRemarks": None
            }
        else:
            return None 
    except pyodbc.Error as e:
        log_print(f"Error getting SyncMeta entry for {branch_name}:{table_name}: {e}", level="error")
        raise

def update_last_synced_value(cursor: pyodbc.Cursor, branch_name: str, table_name: str, last_value: str):
    """
    Updates ONLY the LastValue and LastSynced timestamp for a table and branch.
    Uses the provided cursor and does NOT commit.
    """
    try:
        # Use GETDATE() for LastSynced
        cursor.execute(
            "UPDATE [sync].[SyncMeta] SET LastValue = ?, LastSynced = GETDATE() WHERE BranchName = ? AND TableName = ?",
            (str(last_value), branch_name, table_name)
        )
        if cursor.rowcount == 0:
            # This should ideally not happen if get_sync_meta_entry created the row.
            # This indicates a potential logic flaw or concurrent deletion.
            log_print(f"CRITICAL: No row found in SyncMeta to update LastValue for {branch_name}:{table_name}. "
                      f"This implies the row was not created by get_sync_meta_entry or was deleted.", level="critical")
            # Avoid inserting here as it might hide a problem. The row should exist.
            # If it must be robust against this, an insert could be attempted, but it's a sign of an issue.
            # raise pyodbc.Error(f"SyncMeta row not found for {branch_name}:{table_name} during LastValue update.")
    except pyodbc.Error as e:
        log_print(f"Error updating last synced value for {branch_name}:{table_name} to {last_value}: {e}", level="error")
        raise

def update_sync_meta_status(cursor: pyodbc.Cursor, branch_name: str, table_name: str, status: str, remarks: str | None = None):
    """
    Updates the SyncStatus, LastCompletionTime (if status is 'Complete'), and SyncRemarks.
    Uses the provided cursor and does NOT commit. Uses GETDATE() for timestamps.
    """
    try:
        if status == 'Complete':
            sql = """
                UPDATE [sync].[SyncMeta]
                SET SyncStatus = ?, LastCompletionTime = GETDATE(), SyncRemarks = ?, LastSynced = GETDATE()
                WHERE BranchName = ? AND TableName = ?
            """
            params = (status, remarks, branch_name, table_name)
        else:
            sql = """
                UPDATE [sync].[SyncMeta]
                SET SyncStatus = ?, SyncRemarks = ?, LastSynced = GETDATE() 
                WHERE BranchName = ? AND TableName = ?
            """
            params = (status, remarks, branch_name, table_name)
        
        cursor.execute(sql, params)
        
        if cursor.rowcount == 0:
            # This is critical. get_sync_meta_entry should have created the row if it didn't exist,
            # and that creation should be part of the same transaction that this update is in,
            # or committed before this function is called in a new transaction.
            log_print(f"CRITICAL: SyncMeta row for {branch_name}:{table_name} was expected but not found for status update to '{status}'. "
                      "The row should have been created by get_sync_meta_entry.", level="critical")
            # To prevent silent failures or data loss, it's better to raise an error here.
            # The calling function should ensure the row exists before calling update_sync_meta_status.
            # raise pyodbc.Error(f"SyncMeta row not found for {branch_name}:{table_name} during status update.")
    except pyodbc.Error as e:
        log_print(f"Error updating SyncMeta status for {branch_name}:{table_name} to {status}: {e}", level="error")
        raise

def get_table_schema_details(db_cursor: pyodbc.Cursor, table_name: str, table_schema_name: str = 'dbo') -> dict | None:
    """
    Fetches detailed schema information for a given table.
    SQL Server 2008 Compatible (uses CASE instead of IIF).
    """
    schema_sql = """
    SELECT
        C.TABLE_SCHEMA,
        C.TABLE_NAME,
        C.COLUMN_NAME,
        C.ORDINAL_POSITION,
        C.DATA_TYPE,
        C.CHARACTER_MAXIMUM_LENGTH AS MAX_LENGTH,
        C.NUMERIC_PRECISION,
        C.NUMERIC_SCALE,
        C.DATETIME_PRECISION,
        C.IS_NULLABLE, 
        C.COLUMN_DEFAULT,
        PK_INFO.CONSTRAINT_NAME AS PK_CONSTRAINT_NAME,
        PK_INFO.IS_PRIMARY_KEY_COLUMN
    FROM
        INFORMATION_SCHEMA.COLUMNS C
    OUTER APPLY (
        SELECT
            TC.CONSTRAINT_NAME,
            CASE WHEN KU.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PRIMARY_KEY_COLUMN -- SQL 2008 Compatible
        FROM
            INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
        INNER JOIN
            INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
                ON TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
                AND KU.TABLE_SCHEMA = C.TABLE_SCHEMA
                AND KU.TABLE_NAME = C.TABLE_NAME
                AND KU.COLUMN_NAME = C.COLUMN_NAME
        WHERE
            TC.TABLE_SCHEMA = C.TABLE_SCHEMA
            AND TC.TABLE_NAME = C.TABLE_NAME
            AND TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
    ) AS PK_INFO
    WHERE
        C.TABLE_NAME = ? AND C.TABLE_SCHEMA = ?
    ORDER BY
        C.ORDINAL_POSITION;
    """
    try:
        db_cursor.execute(schema_sql, table_name, table_schema_name)
        rows = db_cursor.fetchall()
    except pyodbc.Error as e:
        log_print(f"Error fetching schema for table {table_schema_name}.{table_name}: {e}", level="error")
        return None

    if not rows:
        log_print(f"Table {table_schema_name}.{table_name} not found or has no columns.", level="warning")
        return None

    columns_details = {}
    pk_columns_temp = {} 
    pk_constraint_name = None

    for row_data in rows:
        col_name = row_data.COLUMN_NAME
        columns_details[col_name] = {
            'table_schema_name': row_data.TABLE_SCHEMA,
            'data_type': row_data.DATA_TYPE.lower(),
            'max_length': int(row_data.MAX_LENGTH) if row_data.MAX_LENGTH is not None else None,
            'numeric_precision': int(row_data.NUMERIC_PRECISION) if row_data.NUMERIC_PRECISION is not None else None,
            'numeric_scale': int(row_data.NUMERIC_SCALE) if row_data.NUMERIC_SCALE is not None else None,
            'datetime_precision': int(row_data.DATETIME_PRECISION) if row_data.DATETIME_PRECISION is not None else None,
            'is_nullable': True if row_data.IS_NULLABLE == 'YES' else False,
            'column_default': row_data.COLUMN_DEFAULT,
            'ordinal_position': row_data.ORDINAL_POSITION
        }
        if row_data.IS_PRIMARY_KEY_COLUMN == 1:
            pk_columns_temp[col_name] = columns_details[col_name]['ordinal_position']
            if not pk_constraint_name and row_data.PK_CONSTRAINT_NAME:
                pk_constraint_name = row_data.PK_CONSTRAINT_NAME
    
    ordered_pk_columns = []
    if pk_constraint_name: 
        pk_order_sql = """
        SELECT KU.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
            ON TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND TC.TABLE_SCHEMA = KU.TABLE_SCHEMA AND TC.TABLE_NAME = KU.TABLE_NAME
        WHERE TC.TABLE_SCHEMA = ? AND TC.TABLE_NAME = ? AND TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND TC.CONSTRAINT_NAME = ?
        ORDER BY KU.ORDINAL_POSITION;
        """
        try:
            db_cursor.execute(pk_order_sql, table_schema_name, table_name, pk_constraint_name)
            pk_rows = db_cursor.fetchall()
            ordered_pk_columns = [row.COLUMN_NAME for row in pk_rows]
        except pyodbc.Error as e:
            log_print(f"Error fetching PK column order for {table_schema_name}.{table_name} using constraint name: {e}", level="error")
            if pk_columns_temp:
                 ordered_pk_columns = sorted(pk_columns_temp.keys(), key=lambda k: pk_columns_temp[k])
    elif pk_columns_temp: 
         ordered_pk_columns = sorted(pk_columns_temp.keys(), key=lambda k: pk_columns_temp[k])

    return {
        'columns': columns_details,
        'primary_key_columns': ordered_pk_columns,
        'primary_key_constraint_name': pk_constraint_name
    }

def get_sql_type_definition(col_details: dict) -> str:
    """
    Constructs a SQL Server type definition string from column details.
    Adjusted for SQL Server 2008 compatibility where applicable.
    """
    dtype = col_details['data_type'].lower()
    
    if dtype in ['nvarchar', 'varchar', 'nchar', 'char', 'binary', 'varbinary']:
        max_len = col_details.get('max_length')
        return f"{dtype.upper()}({max_len if max_len != -1 and max_len is not None else 'MAX'})"
    elif dtype in ['decimal', 'numeric']:
        precision = col_details.get('numeric_precision', 18) 
        scale = col_details.get('numeric_scale', 0)       
        return f"{dtype.upper()}({precision}, {scale})"
    elif dtype == 'datetime2': # DATETIME2 is available in SQL Server 2008
        precision = col_details.get('datetime_precision', 7) 
        return f"DATETIME2({precision})"
    elif dtype == 'datetimeoffset': # DATETIMEOFFSET is available in SQL Server 2008
        precision = col_details.get('datetime_precision', 7)
        return f"DATETIMEOFFSET({precision})"
    elif dtype == 'time': # TIME is available in SQL Server 2008
        precision = col_details.get('datetime_precision', 7)
        return f"TIME({precision})"
    elif dtype == 'date': # DATE is available in SQL Server 2008
        return "DATE"
    elif dtype == 'datetime': # Original DATETIME type
        return "DATETIME"
    elif dtype == 'smalldatetime':
        return "SMALLDATETIME"
    elif dtype == 'float':
        precision = col_details.get('numeric_precision') 
        return f"FLOAT({precision})" if precision and precision <= 53 else "FLOAT" 
    else: 
        return dtype.upper()
