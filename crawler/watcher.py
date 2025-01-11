import time
from pathlib import Path
from crawler.crawlerconfig import CRAWLER_CONFIG
import threading
import os

def _wait_for_file_complete(filepath, stabilization_time=10, check_interval=5, abandonment_time=1800):
    """
    Wait until the file size stabilizes and is not modified for a certain duration.
    Detect abandoned copy operations after prolonged inactivity.

    :param filepath: Path of the file to check
    :param stabilization_time: Time (in seconds) with no modifications before considering the file ready
    :param check_interval: Interval (in seconds) between file size checks
    :param abandonment_time: Maximum time (in seconds) with no activity before considering the file abandoned
    :return: True if the file is ready for processing, False if the copy operation is abandoned
    """
    print(f"Waiting for file to complete: {filepath}")
    last_size = -1  # Track the last observed file size
    last_activity_time = time.time()  # Track the last time the file size changed

    while True:
        try:
            # Ensure the file is accessible
            if not os.access(filepath, os.R_OK):
                print(f"File {filepath} is not accessible yet.")
                time.sleep(check_interval)
                continue

            # Get current file size and modification time
            current_size = os.path.getsize(filepath)
            current_modified_time = os.stat(filepath).st_mtime

            # Detect incremental size changes
            if last_size >= 0:
                increment = current_size - last_size
                if increment > 0:
                    print(f"Copied: +{increment} bytes | Total: {current_size} bytes.")
                    last_activity_time = time.time()  # Update activity timer
                else:
                    # Check for abandonment if no size change
                    if (time.time() - last_activity_time) > abandonment_time:
                        print(f"File copy abandoned after {abandonment_time} seconds of inactivity: {filepath}")
                        return False
            else:
                print(f"Current file size: {current_size} bytes.")

            # Check if the file has stabilized
            if current_size == last_size and (time.time() - current_modified_time) >= stabilization_time:
                print(f"File stabilized: {filepath} with size {current_size} bytes.")
                return True

            # Update last observed file size
            last_size = current_size

        except (OSError, PermissionError) as e:
            print(f"Error accessing file {filepath}: {e}")

        time.sleep(check_interval)


def poll_folder(callback=None):
    """
    Poll the uploads folder for new .csv files that are updated after the script starts
    and trigger a callback for each new file.
    """
    # Define the folder to watch
    directory_to_watch = Path(CRAWLER_CONFIG["Fields_FOLDER"])

    print(f"Polling folder: {directory_to_watch} for new csv files updated after the script starts...")
    seen_files = set()
    script_start_time = time.time()  # Record the script's start time

    while True:
        # try:
        # Get all .csv files in the folder
        current_files = {
            f for f in directory_to_watch.iterdir()
            if f.is_file() and f.suffix == ".csv"
        }

        # Detect new files
        new_files = current_files - seen_files
        for file in new_files:
            print(f"New file detected: {file}")

            # Wait for the file to stabilize
            if _wait_for_file_complete(file):
                if callback:
                    callback(file)
            else:
                print(f"File not ready: {file}")
        # Update seen files
        seen_files.update(new_files)

        # except Exception as e:
        #     print(f"Error during polling: {e}")

        time.sleep(5)  # Poll every 5 seconds
def start_polling_thread(callback=None):
    """
    Start the poll_folder function in a new thread.
    """
    polling_thread = threading.Thread(target=poll_folder, args=(callback,), daemon=True)
    polling_thread.start()
    return polling_thread

def poll_table(callback=None):
    """
    Polls a DuckDB table at regular intervals and processes the results.

    Args:
        con: DuckDB connection object.
        query (str): SQL query to execute.
        interval (int): Time interval (in seconds) between polls.
        callback (function, optional): Function to process query results.
    """
    print("Starting table polling...")
    try:
        while True:

            if callback:
                callback()

            # Wait for the next poll
            time.sleep(10)
    except KeyboardInterrupt:
        print("\nPolling stopped by user.")