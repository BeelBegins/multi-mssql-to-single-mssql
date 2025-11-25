import pyodbc
import threading
import logging
import traceback
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.db_utils import (
    connect_to_db,
    ensure_database_exists, 
    ensure_sync_schema_and_meta, 
    get_sync_meta_entry,
    update_last_synced_value,
    update_sync_meta_status,
    get_table_schema_details,
    get_sql_type_definition
)
from utils.common import (
    log_print,
    TABLES_TO_SYNC, 
    SYNC_METHODS,
    CONSOLIDATED_TARGET_DATABASE,
    MAX_CONCURRENT_TABLES_PER_BRANCH
)
from utils.schema_manager import align_target_schema_to_source, BRANCH_ID_COL
from utils.common import build_query as common_build_query

def load_connections(file_path="connection_strings.txt"):
    """Loads connections. No changes needed here."""
    conns = []
    try:
        with open(file_path, 'r') as f:
            lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        for line_num, line in enumerate(lines, 1):
            parts = [p.strip() for p in line.split(',')]
            if len(parts) == 5:
                server, db, user, pwd, tgt_flag = parts
                port = '1433'
            elif len(parts) == 6:
                server, port, db, user, pwd, tgt_flag = parts
            else:
                log_print(f"Skipping malformed line {line_num} in {file_path}", level="warning")
                continue
            conns.append({
                'server': server, 'port': port, 'database': db,
                'username': user, 'password': pwd, 'target_flag': tgt_flag.lower()
            })
        log_print(f"Loaded {len(conns)} connection configurations.", level="info")
    except FileNotFoundError:
        log_print(f"Connection file '{file_path}' not found.", level="critical")
        raise
    return conns

def db_config(cfg: dict) -> dict:
    """Extracts db connection params. No changes needed."""
    return {k: v for k, v in cfg.items() if k in ('server', 'port', 'database', 'username', 'password')}

def _get_branch_name(source_db_cursor: pyodbc.Cursor, source_config: dict) -> str:
    """
    Determines the branch name from the 'Logo' table, or falls back to the unique database name.
    """
    try:
        source_db_cursor.execute("SELECT TOP 1 BOTMESS1 FROM Logo WITH (NOLOCK)")
        row = source_db_cursor.fetchone()
        if row and row[0] and str(row[0]).strip():
            branch_name = str(row[0]).strip().lower()
            log_print(f"Determined branch name from Logo table for DB '{source_config['database']}': {branch_name}", level="info")
            return branch_name
        else:
            log_print(f"BOTMESS1 from Logo table is NULL or empty in DB '{source_config['database']}'. Using database name as fallback.", level="warning")
    except pyodbc.Error as e:
        log_print(f"Could not fetch branch name from Logo table in DB '{source_config['database']}' (Error: {e}). Using database name as fallback.", level="warning")
    
    fallback_branch_name = source_config['database'].lower()
    log_print(f"Using database name as fallback branch name: {fallback_branch_name}", level="info")
    return fallback_branch_name

def _determine_watermark_and_pk_columns(table_name: str, source_schema_details: dict) -> tuple[str | None, str | None]:
    """Determines watermark/PK cols. No changes needed."""
    if not source_schema_details or not source_schema_details['primary_key_columns']:
        log_print(f"Cannot determine PK for table {table_name}, schema info missing.", level="error")
        return None, None
    
    pk_col_for_merge = source_schema_details['primary_key_columns'][0]
    sync_method_config = SYNC_METHODS.get(table_name.lower(), 'autono')
    watermark_col_for_query = pk_col_for_merge

    if sync_method_config not in ['autono', 'timestamp', 'hybrid', 'full']:
        watermark_col_for_query = sync_method_config
    elif sync_method_config == 'timestamp':
        if table_name.lower() in ('saledetail', 'saleheader'):
             watermark_col_for_query = 'TrnDate'
        elif table_name.lower() == 'debitheader':
             watermark_col_for_query = 'VoucherDate'
    
    return watermark_col_for_query, pk_col_for_merge

def _upsert_batch_atomic(
    target_cursor: pyodbc.Cursor, 
    table_name: str,
    columns_in_batch: list[str],
    batch_rows: list[pyodbc.Row],
    pk_col_for_merge: str, 
    branch_identifier: str,
    current_batch_last_val_for_meta: str,
    source_schema_details: dict
):
    """
    Upserts a batch of data using a temporary table for performance and memory efficiency.
    """
    if not batch_rows:
        return

    temp_table_name = f"##{table_name}_sync_{threading.get_ident()}"

    try:
        # 1. Create the temporary table
        column_definitions = [f"[{BRANCH_ID_COL}] NVARCHAR(255) NOT NULL"]
        for col_name in columns_in_batch:
            col_details = source_schema_details['columns'][col_name]
            col_type_def = get_sql_type_definition(col_details)
            column_definitions.append(f"[{col_name}] {col_type_def}")
        
        create_temp_table_sql = f"CREATE TABLE {temp_table_name} ({', '.join(column_definitions)})"
        target_cursor.execute(create_temp_table_sql)

        # 2. Insert the batch into the temporary table
        target_columns = [BRANCH_ID_COL] + columns_in_batch
        data_to_insert = [(branch_identifier,) + tuple(row) for row in batch_rows]
        
        insert_sql = f"INSERT INTO {temp_table_name} ({', '.join(f'[{c}]' for c in target_columns)}) VALUES ({', '.join(['?'] * len(target_columns))})"
        
        target_cursor.fast_executemany = True
        
        start_staging = time.perf_counter()
        target_cursor.executemany(insert_sql, data_to_insert)
        staging_time = time.perf_counter() - start_staging
        log_print(f"Staged {len(data_to_insert)} rows to temp table in {staging_time:.2f}s.", level="debug")

        # 3. Execute the server-side MERGE from the temporary table
        set_clause_parts = [f"target.[{c}] = source.[{c}]" for c in columns_in_batch if c.lower() != pk_col_for_merge.lower()]
        set_clause = ', '.join(set_clause_parts) if set_clause_parts else f"target.[{pk_col_for_merge}] = source.[{pk_col_for_merge}]"

        merge_on_clause = f"target.[{BRANCH_ID_COL}] = source.[{BRANCH_ID_COL}] AND target.[{pk_col_for_merge}] = source.[{pk_col_for_merge}]"
        
        target_col_list_sql = ', '.join(f'[{c}]' for c in target_columns)
        source_col_list_sql = ', '.join(f'source.[{c}]' for c in target_columns)

        merge_sql = f"""
        MERGE INTO [{table_name}] AS target
        USING {temp_table_name} AS source
        ON ({merge_on_clause})
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED BY TARGET THEN INSERT ({target_col_list_sql}) VALUES ({source_col_list_sql});
        """
        
        start_merge = time.perf_counter()
        target_cursor.execute(merge_sql)
        merge_time = time.perf_counter() - start_merge
        log_print(f"Merged from temp table in {merge_time:.2f}s.", level="debug")
        
        # 4. Update metadata
        update_last_synced_value(target_cursor, branch_identifier, table_name, current_batch_last_val_for_meta)

    finally:
        # 5. Clean up by dropping the temporary table
        try:
            target_cursor.execute(f"DROP TABLE {temp_table_name}")
        except pyodbc.Error:
            pass

def sync_table(table_to_sync: str, source_branch_config: dict, target_server_config: dict, branch_identifier: str, running_state: dict):
    """
    Main function to sync a single table, with detailed timing logs.
    """
    thread_name = threading.current_thread().name
    log_print(f"[{thread_name}] Starting sync for table: {table_to_sync}, branch: {branch_identifier}", level="info")

    target_db_name = CONSOLIDATED_TARGET_DATABASE
    src_conn = None
    tgt_conn = None

    try:
        # --- Initial Setup and Connection ---
        if not running_state['is_running']: return

        ensure_database_exists(master_conn_details=db_config(target_server_config), db_name=target_db_name)

        src_conn = connect_to_db(**db_config(source_branch_config))
        tgt_conn = connect_to_db(**{**db_config(target_server_config), 'database': target_db_name, 'autocommit': False})

        # --- Metadata and Schema Setup ---
        ensure_sync_schema_and_meta(tgt_conn)
        with tgt_conn.cursor() as t_meta_cursor:
            get_sync_meta_entry(t_meta_cursor, branch_identifier, table_to_sync, create_if_not_exists=True)
            update_sync_meta_status(t_meta_cursor, branch_identifier, table_to_sync, 'InProgress', f'[{thread_name}] Starting sync cycle.')
            tgt_conn.commit()

        if not running_state['is_running']: return

        schema_aligned = False
        with src_conn.cursor() as s_cursor, tgt_conn.cursor() as t_align_cursor:
            schema_aligned = align_target_schema_to_source(s_cursor, t_align_cursor, table_to_sync)
        
        if not schema_aligned:
            log_print(f"[{thread_name}] Skipping data sync for {table_to_sync} due to schema alignment issues.", level="warning")
            with tgt_conn.cursor() as t_err_cursor:
                update_sync_meta_status(t_err_cursor, branch_identifier, table_to_sync, 'SchemaError', "Schema alignment failed.")
                tgt_conn.commit()
            return

        # --- Prepare for Data Sync Loop ---
        with src_conn.cursor() as s_cursor: 
            source_schema_details = get_table_schema_details(s_cursor, table_to_sync, 'dbo')
        
        watermark_col, pk_col_for_merge = _determine_watermark_and_pk_columns(table_to_sync, source_schema_details)
        if not watermark_col or not pk_col_for_merge:
             raise ValueError(f"Could not determine watermark or PK column for {table_to_sync}.")

        ordered_source_cols_tuples = sorted(source_schema_details['columns'].items(), key=lambda item: item[1]['ordinal_position'])
        select_cols_for_query = [col_name for col_name, _ in ordered_source_cols_tuples]

        with tgt_conn.cursor() as t_meta_cursor:
            current_meta_entry = get_sync_meta_entry(t_meta_cursor, branch_identifier, table_to_sync, create_if_not_exists=False)
        
        query_last_val = current_meta_entry['LastValue']
        sync_method_for_table = SYNC_METHODS.get(table_to_sync.lower(), 'autono')
        if sync_method_for_table == 'full':
            query_last_val = '0'

        # --- Data Sync Loop with Batch Commits ---
        total_rows_synced_this_run = 0
        data_sync_loop_error = None
        
        try:
            while running_state['is_running']:
                query = common_build_query(
                    table_name=table_to_sync, select_columns=select_cols_for_query,
                    watermark_column=watermark_col, last_synced_value=query_last_val,
                    sync_method=sync_method_for_table
                )
                
                start_fetch = time.perf_counter()
                with src_conn.cursor() as src_data_cursor:
                    src_data_cursor.execute(query)
                    rows = src_data_cursor.fetchall()
                    if rows: 
                        actual_cols_from_query = [col_desc[0] for col_desc in src_data_cursor.description]
                fetch_time = time.perf_counter() - start_fetch
                
                if not rows:
                    log_print(f"[{thread_name}] No more new rows for {table_to_sync}. Sync for this table is complete.", level="info")
                    break
                
                log_print(f"Fetched {len(rows)} rows from source in {fetch_time:.2f}s.", level="debug")

                next_last_val_for_meta = str(max(row[actual_cols_from_query.index(watermark_col)] for row in rows))

                # Each batch is its own transaction
                with tgt_conn.cursor() as t_data_cursor:
                    _upsert_batch_atomic( 
                        t_data_cursor, table_to_sync, actual_cols_from_query, rows,
                        pk_col_for_merge, branch_identifier, next_last_val_for_meta,
                        source_schema_details
                    )
                tgt_conn.commit() # COMMIT THE BATCH!

                total_rows_synced_this_run += len(rows)
                log_print(f"[{thread_name}] Committed batch for {branch_identifier}:{table_to_sync} ({len(rows)} rows). New watermark: {next_last_val_for_meta}", "info")
                
                if sync_method_for_table == 'full':
                    break
                
                query_last_val = next_last_val_for_meta
        
        except Exception as loop_err:
            data_sync_loop_error = loop_err
            log_print(f"[{thread_name}] Error during data sync loop for {branch_identifier}:{table_to_sync}. See full traceback below.", level="error")
            log_print(traceback.format_exc(), level="error")
            try:
                tgt_conn.rollback()
            except pyodbc.Error as rb_err:
                log_print(f"Rollback failed after error: {rb_err}", level="error")

        # --- Final Status Update ---
        if not running_state['is_running']:
             log_print(f"[{thread_name}] Shutdown initiated. Final status for {table_to_sync} will be 'Pending'.", "warning")
             data_sync_loop_error = InterruptedError("Shutdown signaled")

        final_status = 'Complete'
        remarks = f'[{thread_name}] Sync cycle completed. {total_rows_synced_this_run} rows processed.'
        
        if data_sync_loop_error:
            final_status = 'Pending' if total_rows_synced_this_run > 0 else 'Failed'
            remarks = f"[{thread_name}] Sync interrupted: {str(data_sync_loop_error)[:1000]}"

        with tgt_conn.cursor() as t_final_cursor:
            update_sync_meta_status(t_final_cursor, branch_identifier, table_to_sync, final_status, remarks)
        tgt_conn.commit()
        log_print(f"[{thread_name}] Final status for {table_to_sync} set to '{final_status}'.", level="success" if final_status == 'Complete' else 'warning')

    except Exception as e:
        log_print(f"[{thread_name}] CRITICAL failure in sync_table for {table_to_sync}, branch {branch_identifier}: {e}", level="critical")
        log_print(traceback.format_exc(), level="debug")
        if tgt_conn:
            try:
                with tgt_conn.cursor() as t_crit_cursor:
                    update_sync_meta_status(t_crit_cursor, branch_identifier, table_to_sync, 'Failed', f"[{thread_name}] Unexpected error: {str(e)[:1000]}")
                tgt_conn.commit()
            except: pass
    finally:
        if src_conn: src_conn.close()
        if tgt_conn: tgt_conn.close()


def sync_branch(source_branch_config: dict, target_server_config: dict, running_state: dict):
    """
    Orchestrates the sync for a single source branch into the consolidated database.
    """
    source_conn_params = db_config(source_branch_config)
    branch_identifier = None
    
    try:
        if not running_state['is_running']: return
        with connect_to_db(**source_conn_params) as temp_conn, temp_conn.cursor() as cur:
            branch_identifier = _get_branch_name(cur, source_branch_config)
    except Exception as e:
        log_print(f"Could not connect to source {source_conn_params.get('server')}/{source_conn_params.get('database')} to get branch name: {e}. Skipping.", level="error")
        return

    log_print(f"Branch '{branch_identifier}': Starting sync process.", level="info")

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_TABLES_PER_BRANCH, thread_name_prefix=f"{branch_identifier}_sync") as executor:
        futures = {executor.submit(sync_table, table, source_branch_config, target_server_config, branch_identifier, running_state): table for table in TABLES_TO_SYNC}
        
        for future in as_completed(futures):
            table_name = futures[future]
            try:
                future.result()
            except Exception as exc:
                log_print(f"Branch '{branch_identifier}': Sync for table {table_name} generated a critical exception: {exc}", level="error")
                log_print(traceback.format_exc(), level="debug")

    log_print(f"Branch '{branch_identifier}': Completed all table sync operations.", level="success")
