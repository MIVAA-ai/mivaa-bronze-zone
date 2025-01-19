from config.logger_config import configure_logger
from crawler import start_polling_thread, poll_table
from file_processor.file_processor_registry import FileProcessorRegistry
from utils.db_util import get_session, get_columns_from_store
from models.files import insert_data, fetch_files_to_process, update_file_status
import pandas as pd

# Configure logger
logger = configure_logger(__name__)

def insert_fields_data_in_db(filepath):
    """
    Insert field data into the database.

    :param filepath: Path to the file to insert data from.
    """
    with get_session() as session:
        try:
            logger.info(f"Inserting data from file: {filepath}")
            insert_data(session, str(filepath), 'FIELD', '')
            logger.info("Data insertion completed successfully.")
        except Exception as e:
            logger.error(f"Error inserting data from file {filepath}: {e}")

def read_fields_data_in_db():
    """
    Read data from the database, validate it, and update file statuses.
    """
    with get_session() as session:
        try:
            logger.info("Fetching files to process.")
            results = fetch_files_to_process(session)
            if results is None:
                logger.info("No files to process.")
                return

            logger.info(f"Processing file: {results.filepath} with file_status {results.file_status}")
            df = pd.read_csv(results.filepath)

            file_processor = FileProcessorRegistry.get_processor(results.datatype)

            if results.file_status == 'BRONZE_PROCESSED':
                logger.info("Updating file status to 'SILVER_PROCESSING'.")
                update_file_status(session, 'SILVER_PROCESSING', results.id)
                return
            # Validate columns
            if file_processor.validate_columns(df):
                logger.error("Column validation failed. Updating file status to error.")
                update_file_status(session, 'ERROR', results.id, "Error: Columns do not match")
                return
            else:
                logger.info("Column validation passed. Updating file status to 'BRONZE_PROCESSING'.")
                update_file_status(session, 'BRONZE_PROCESSING', results.id)

                file_processor.validate(df, results)
                logger.info("Field validation completed successfully. Updating file status to 'BRONZE_PROCESSED'.")
                update_file_status(session, 'BRONZE_PROCESSED', results.id)
                return
        except Exception as e:
            logger.error(f"An error occurred while processing files: {e}")

def start_app():
    """
    Main entry point for executing the database initialization script.
    """
    # Start the polling thread and begin processing
    try:
        logger.info("Starting polling thread for data insertion.")
        start_polling_thread(insert_fields_data_in_db)
        logger.info("Polling thread started successfully.")
        poll_table(read_fields_data_in_db)
    except Exception as e:
        logger.error(f"An error occurred during polling: {e}")
