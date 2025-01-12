from config.logger_config import configure_logger
from utils.generate_sqlalchemy_model import generate_model_for_table

# Configure logger
logger = configure_logger("error_messages.log")

ErrorMessagesModel = None
# Generate the SQLAlchemy model class dynamically for the 'validation_errors' table
try:
    if ErrorMessagesModel is None:
        ErrorMessagesModel = generate_model_for_table('error_messages')
        logger.info(f"Generated model class for table: {ErrorMessagesModel.__tablename__}")
except Exception as e:
    logger.error(f"Error generating model class for table 'error_messages': {e}")
    # Ensure ValidationErrorsModel is defined as None if generation fails