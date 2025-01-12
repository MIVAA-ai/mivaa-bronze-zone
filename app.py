from config.logger_config import configure_logger
from crawler import start_polling_thread, poll_table
from validators.field_data_validator import validate_field
from utils.db_util import get_session, get_columns_from_store
from models.files import insert_data, fetch_files_to_process, update_file_status
import pandas as pd

# Configure logger
logger = configure_logger(__name__)

# Fetch column list for the 'field_bronze_table'
field_column_list = get_columns_from_store('field_bronze_table')
logger.info(f"Fetched column list for 'field_bronze_table': {field_column_list}")

def insert_fields_data_in_db(filepath):
    """
    Insert field data into the database.

    :param filepath: Path to the file to insert data from.
    """
    with get_session() as session:
        try:
            logger.info(f"Inserting data from file: {filepath}")
            insert_data(session, str(filepath), 'field', '')
            logger.info("Data insertion completed successfully.")
        except Exception as e:
            logger.error(f"Error inserting data from file {filepath}: {e}")

def validate_columns(df, column_list):
    """
    Validate that the required columns exist in the DataFrame.

    :param df: DataFrame to validate.
    :return: Count of missing columns.
    """
    missing_columns = [col for col in column_list if col not in df.columns]
    if missing_columns:
        logger.warning(f"Missing columns: {missing_columns}")
    return len(missing_columns)

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

            logger.info(f"Processing file: {results.filepath}")
            df = pd.read_csv(results.filepath)

            # Validate columns
            if validate_columns(df, field_column_list):
                logger.error("Column validation failed. Updating file status to error.")
                update_file_status(session, '4', results.id, "Error: Columns do not match")
            else:
                logger.info("Column validation passed. Updating file status to processing.")
                update_file_status(session, '2', results.id)

                # Perform field validation
                validate_field(df, results.id, results.filename)
                logger.info("Field validation completed successfully. Updating file status to complete.")
                update_file_status(session, '3', results.id)
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
