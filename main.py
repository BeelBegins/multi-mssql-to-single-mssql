import os
import signal
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from utils.sync_utils import sync_branch, load_connections # db_config is in sync_utils now
from utils.common import log_print
from sync_config import allowed_start_time, allowed_end_time

MAX_DB_SYNC_WORKERS = 4 # Renamed from MAX_WORKERS for clarity
RUN_INTERVAL_SECONDS = 2000 
ALLOWED_WINDOW_CHECK_INTERVAL_SECONDS = 60

# Global mutable state for controlling the main loop and threads
running_state = {'is_running': True}

def setup_logging():
    """Configures logging for the application."""
    log_dir = "log"
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO, 
        format="%(asctime)s - %(levelname)s - %(name)s - %(threadName)s - %(message)s", # Added threadName
        encoding='utf-8',
        handlers=[
            logging.FileHandler(os.path.join(log_dir, "sync.log"), encoding='utf-8'),
            logging.StreamHandler() # Also log to console
        ]
    )

    # Configure specific logger for successful operations
    success_logger = logging.getLogger("success")
    success_handler = logging.FileHandler(os.path.join(log_dir, "success.log"), encoding='utf-8')
    success_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    success_logger.addHandler(success_handler)
    success_logger.setLevel(logging.INFO)
    success_logger.propagate = False # Avoid duplicate messages in root logger's sync.log

    # Configure specific logger for errors
    error_logger = logging.getLogger("errors")
    error_handler = logging.FileHandler(os.path.join(log_dir, "errors.log"), encoding='utf-8')
    error_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(threadName)s - %(message)s")) # Added threadName
    error_logger.addHandler(error_handler)
    error_logger.setLevel(logging.ERROR) 
    error_logger.propagate = False # Avoid duplicate messages in root logger's sync.log

    # Ensure log_print uses the root logger for general messages
    # The log_print in common.py uses logging.info, logging.error etc. which go to root.
    log_print("Logging configured.", level="info")


def handle_exit(signum, frame):
    """Signal handler for graceful shutdown."""
    global running_state # Use the shared mutable state
    if running_state['is_running']: 
        log_print(f"🛑 Signal {signal.Signals(signum).name} received. Gracefully shutting down after current tasks...", level="warning")
        running_state['is_running'] = False
    else:
        log_print(f"🛑 Shutdown already in progress. Signal {signal.Signals(signum).name} received again.", level="warning")


def in_allowed_sync_window():
    """Checks if the current time is within the allowed synchronization window."""
    try:
        now_time = datetime.now().time()
        start_time_obj = datetime.strptime(allowed_start_time, "%H:%M").time()
        end_time_obj = datetime.strptime(allowed_end_time, "%H:%M").time()

        if start_time_obj == end_time_obj: 
            return True
        if start_time_obj < end_time_obj: 
            return start_time_obj <= now_time < end_time_obj
        else: 
            return now_time >= start_time_obj or now_time < end_time_obj
    except ValueError as e:
        log_print(f"Error parsing allowed_start_time/allowed_end_time from sync_config.py: {e}. Defaulting to allowing sync.", level="error")
        return True


def main_sync_cycle(current_running_state: dict): # Accept running_state
    """Performs one full cycle of syncing all source branches."""
    log_print("🚀 Starting new DBSync cycle...", level="info")
    try:
        # load_connections is in sync_utils
        connection_configs = load_connections("connection_strings.txt") 
        
        target_config = next((cfg for cfg in connection_configs if cfg.get('target_flag','no') == 'yes'), None)
        source_configs = [cfg for cfg in connection_configs if cfg.get('target_flag','no') != 'yes']

        if not target_config:
            log_print("❌ No target connection found in connection_strings.txt. Cycle aborted.", level="critical")
            return
        if not source_configs:
            log_print("❌ No source connections found in connection_strings.txt. Cycle aborted.", level="warning")
            return

        log_print(f"Target server: {target_config['server']}. Found {len(source_configs)} source(s) to process.", level="info")
        
        # db_config is now also in sync_utils, but target_server_config is used directly by sync_branch
        # target_server_params = db_config(target_config) # Not needed if passing full target_config

        with ThreadPoolExecutor(max_workers=MAX_DB_SYNC_WORKERS) as executor:
            future_to_src = {
                # Pass current_running_state to sync_branch
                executor.submit(sync_branch, src_cfg, target_config, current_running_state): src_cfg.get('server', 'UnknownServer')
                for src_cfg in source_configs
            }

            for future in as_completed(future_to_src):
                src_server = future_to_src[future]
                try:
                    future.result() 
                    log_print(f"Branch sync completed for source: {src_server}", level="info")
                except Exception as exc:
                    log_print(f"Branch sync for source {src_server} generated an exception: {exc}", level="error")
                    logging.exception(f"Traceback for {src_server} exception:") # Log full traceback
                if not current_running_state['is_running']: # Check shared state
                    log_print("Shutdown signal received, attempting to cancel pending tasks.", level="warning")
                    # Cancel futures that haven't started
                    for f in future_to_src:
                        if not f.done():
                            f.cancel()
                    # Note: Tasks already running will complete unless they internally check running_state
                    break 
        
        log_print("✅ DBSync cycle completed.", level="success")

    except FileNotFoundError:
        log_print("❌ CRITICAL: connection_strings.txt not found. Please create it. Cycle aborted.", level="critical")
    except Exception as e:
        log_print(f"❌ Fatal error in main_sync_cycle: {e}", level="critical")
        logging.exception("Exception details from main_sync_cycle:")


if __name__ == "__main__":
    setup_logging() 

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    log_print("🚀 DBSync Script Started. Press Ctrl+C to initiate graceful shutdown.", level="info")

    while running_state['is_running']: # Check shared state
        if not in_allowed_sync_window():
            log_print(f"🕒 Outside allowed sync window ({allowed_start_time}-{allowed_end_time}). Waiting for {ALLOWED_WINDOW_CHECK_INTERVAL_SECONDS}s...", level="info")
            
            for _ in range(ALLOWED_WINDOW_CHECK_INTERVAL_SECONDS // 5): 
                if not running_state['is_running']: break # Check shared state
                time.sleep(20000)
            if not running_state['is_running']: break
            continue

        main_sync_cycle(running_state) # Pass shared state

        if running_state['is_running']: 
            log_print(f"⏳ Waiting {RUN_INTERVAL_SECONDS} seconds before next cycle...", level="info")
            for _ in range(RUN_INTERVAL_SECONDS):
                if not running_state['is_running']: break # Check shared state
                time.sleep(1)
        if not running_state['is_running']: break 

    log_print("✅ DBSync Script has shut down gracefully.", level="info")
