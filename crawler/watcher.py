import time
from pathlib import Path
from crawler.crawlerconfig import CRAWLER_CONFIG
import threading
import os


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
        try:
            # Get all .csv files in the folder
            current_files = {
                f for f in directory_to_watch.iterdir()
                if f.is_file() and f.suffix == ".csv"
            }

            # Detect new files
            new_files = current_files - seen_files
            for file in new_files:
                if callback:
                    callback(file)
                else:
                    print(f"New file detected: {file}")

            # Update seen files
            seen_files.update(new_files)

        except Exception as e:
            print(f"Error during polling: {e}")

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