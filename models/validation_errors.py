from config.logger_config import configure_logger
from utils.db_util import get_session, text
from sqlalchemy import func
from datetime import datetime
from utils.generate_sqlalchemy_model import generate_model_for_table

# Configure logger
logger = configure_logger("validation_errors.log")

ValidationErrorsModel = None
# Generate the SQLAlchemy model class dynamically for the 'validation_errors' table
try:
    if ValidationErrorsModel is None:
        ValidationErrorsModel = generate_model_for_table('validation_errors')
        logger.info(f"Generated model class for table: {ValidationErrorsModel.__tablename__}")
except Exception as e:
    logger.error(f"Error generating model class for table 'validation_errors': {e}")
    # Ensure ValidationErrorsModel is defined as None if generation fails

def log_errors_to_db(errors: list, file_id: int):
    """
    Log validation errors to the database dynamically using the ValidationErrorsModel.

    :param errors: List of dictionaries containing validation error details.
    :param file_id: ID of the file associated with the errors.
    """
    if ValidationErrorsModel is None:
        logger.error("ValidationErrorsModel is not defined. Cannot log errors.")
        return

    with get_session() as session:
        # Handle empty errors list
        if not errors:
            logger.info("No errors to log.")
            return

        logger.info("Logging errors to the database...")

        # Fetch the maximum existing error_id and calculate new IDs
        max_id = session.query(func.max(ValidationErrorsModel.error_id)).scalar() or 0
        new_error_id_start = max_id + 1

        # Add required fields to each error
        for idx, error in enumerate(errors):
            error["error_id"] = new_error_id_start + idx
            error["file_id"] = file_id
            error["created_at"] = datetime.now()

        # Create instances of the ValidationErrorsModel
        error_records = [ValidationErrorsModel(**error) for error in errors]

        try:
            # Add the records to the session and commit
            session.bulk_save_objects(error_records)
            session.commit()
            logger.info(f"{len(errors)} validation errors logged successfully.")
        except Exception as e:
            logger.error(f"Error logging validation errors: {e}")
            session.rollback()
