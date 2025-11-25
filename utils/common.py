import logging
from datetime import datetime, timedelta

# --- Configuration Constants ---

# NEW: Define the single database where all data will be consolidated.
CONSOLIDATED_TARGET_DATABASE = "ConsolidatedDB" 

SYNC_LOOKBACK_DAYS = 0
DEFAULT_BATCH_SIZE = 100
MAX_CONCURRENT_TABLES_PER_BRANCH = 2 

BATCH_SIZE_MAP = {
    'saledetail': 100,
    'debitdetail': 100,
    'saleheader':100,
    'Item': 100,
    'Supplier': 100,
    'logo': 100,
    'tbl_ChartofAccount': 110,
'SubCategory':'110'
}

SYNC_METHODS = {
    'saledetail': 'autono',
    'saleheader': 'Autono',
    'supplier': 'full',
    'debitdetail': 'autono',
    'debitheader': 'VoucherNo',
    'logo': 'full',
    'tbl_chartofaccount': 'full',
    'BallotingSys': 'CouponNo',
    'Item':'full',
    'Brand':'timestamp',
    'ITEMOTHERS':'full',
    'Category':'timestamp',
    'SubCategory':'Autono'
}

TABLES_TO_SYNC = [
    'SALEDETAIL',
    'SALEHEADER',
    #'SALEHEADERCUSTOMER',
    'Item',
    'debitdetail',
    'debitheader',
    'whdebitdetail',
    'whdebitheader',
    'BallotingSys',
    #'logo',
    #'Supplier',
    #'Brand',
    'ITEMOTHERS',
    #'Category',
    'SubCategory'
    'Tbl_V_C',
    'Tbl_V_P',
	'Tbl_ChartOfAccount'
]

OVERRIDE_PK_COL = {}
DIAGNOSTIC_SUPPORT_ENABLED = True

# --- Logging Function (No changes) ---
def log_print(msg, level="info"):
    print(msg, flush=True)
    if level == "error":
        logging.error(msg)
    elif level == "warning":
        logging.warning(msg)
    elif level == "debug":
        logging.debug(msg)
    elif level == "critical":
        logging.critical(msg)
    else:
        logging.info(msg)
    
    if level == "success":
        success_logger = logging.getLogger("success")
        success_logger.info(msg)
    elif level == "error" or level == "critical":
        error_logger = logging.getLogger("errors")
        error_logger.error(msg)


# --- Query Building Function (No changes) ---
def build_query(
    table_name: str,
    select_columns: list[str],
    watermark_column: str,
    last_synced_value: str,
    sync_method: str | None = None
    ) -> str:
    if not select_columns:
        log_print(f"Warning: No columns specified for SELECT in build_query for table '{table_name}'. Defaulting to SELECT *.", level="warning")
        select_columns_str = "*"
    else:
        select_columns_str = ", ".join([f"[{col}]" for col in select_columns])

    batch_size = BATCH_SIZE_MAP.get(table_name.lower(), DEFAULT_BATCH_SIZE)
    
    if sync_method is None:
        sync_method = SYNC_METHODS.get(table_name.lower(), 'autono')

    if sync_method == 'full':
        return f"SELECT TOP {batch_size} {select_columns_str} FROM [{table_name}] ORDER BY [{watermark_column}]"

    conditions = []
    current_time_cutoff_str = (datetime.now() - timedelta(days=SYNC_LOOKBACK_DAYS)).strftime('%Y-%m-%d %H:%M:%S')

    if sync_method in ('autono', 'hybrid') or watermark_column not in ('TrnDate', 'VoucherDate'):
        conditions.append(f"[{watermark_column}] > '{last_synced_value}'")

    if sync_method in ('timestamp', 'hybrid'):
        timestamp_col_for_condition = None
        if table_name.lower() in ('saledetail', 'saleheader') and 'TrnDate' in select_columns:
            timestamp_col_for_condition = 'TrnDate'
        elif table_name.lower() == 'debitheader' and 'VoucherDate' in select_columns:
            timestamp_col_for_condition = 'VoucherDate'
        elif watermark_column in select_columns and SYNC_METHODS.get(table_name.lower()) == 'timestamp':
            timestamp_col_for_condition = watermark_column

        if timestamp_col_for_condition:
            conditions.append(f"[{timestamp_col_for_condition}] >= '{current_time_cutoff_str}'")
        else:
            log_print(f"Warning: Timestamp column for 'timestamp'/'hybrid' sync method not identified for table '{table_name}'.", level="warning")

    if not conditions:
        log_print(f"Warning: No WHERE conditions for incremental sync of table '{table_name}'. Fetching from beginning.", level="warning")
        return f"SELECT TOP {batch_size} {select_columns_str} FROM [{table_name}] ORDER BY [{watermark_column}]"

    where_clause = ' AND '.join(conditions)
    return f"SELECT TOP {batch_size} {select_columns_str} FROM [{table_name}] WHERE {where_clause} ORDER BY [{watermark_column}]"
