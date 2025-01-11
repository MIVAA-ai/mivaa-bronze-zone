import logging

from config.logger_config import configure_logger
from utils.db_util import get_session
from datetime import datetime
import pandas as pd
from utils.generate_sqlalchemy_model import generate_model_for_table

# Configure logging
logger = configure_logger("field_bronze_table.log")

# Generate the SQLAlchemy model class dynamically for the 'field_bronze_table' table
try:
    FieldBronzeTableModel = generate_model_for_table('field_bronze_table')
    logger.info(f"Generated model class for table: {FieldBronzeTableModel.__tablename__}")
except Exception as e:
    logger.error(f"Error generating model class for table 'field_bronze_table': {e}")
    FieldBronzeTableModel = None  # Ensure FieldBronzeTableModel is defined as None if generation fails

def log_field_bronze_table(df: pd.DataFrame, file_id: int, error_index_set):
    """
    Logs validation status for each row in the database into the 'field_bronze_table'.

    Parameters:
    - df (pd.DataFrame): DataFrame containing the data to log.
    - file_id (int): ID of the file being processed.
    - error_index_set (set): Set of indices that failed validation.
    """
    if FieldBronzeTableModel is None:
        logger.error("FieldBronzeTableModel is not defined. Cannot log data.")
        return

    with get_session() as session:
        try:
            # Determine the starting ID for the new rows
            max_id = session.query(FieldBronzeTableModel.id).order_by(FieldBronzeTableModel.id.desc()).first()
            max_id = max_id[0] if max_id else 0

            # Add required columns to the DataFrame
            df["id"] = range(max_id + 1, max_id + 1 + len(df))
            df["row_index"] = df.index
            df["file_id"] = file_id
            df["validation_status"] = [
                "Failed" if idx in error_index_set else "Passed" for idx in df.index
            ]
            df["validation_timestamp"] = datetime.now()

            # Convert the DataFrame to a list of dictionaries
            data_to_insert = df.to_dict(orient="records")

            # Use bulk_insert_mappings for efficient insertion
            session.bulk_insert_mappings(FieldBronzeTableModel, data_to_insert)
            session.commit()
            logger.info("Validation results logged successfully.")
        except Exception as e:
            logger.error(f"Error logging validation results: {e}")
            session.rollback()
