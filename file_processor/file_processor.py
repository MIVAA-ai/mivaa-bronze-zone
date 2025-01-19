from abc import ABC, abstractmethod
from config.logger_config import configure_logger
from config.project_config import PROJECT_CONFIG
from utils.db_util import get_columns_from_store


class FileProcessor(ABC):
    """
    Abstract base class for file processors.
    """
    def __init__(self, file_type):
        self.logger = configure_logger(__name__)  # Fixed typo
        self.bronze_table_name = PROJECT_CONFIG["SQL_TABLES"][file_type]["BRONZE_TABLE"]

        # Fetch column list for the specified bronze table
        self.field_column_list = get_columns_from_store(self.bronze_table_name)
        self.logger.info(f"Fetched column list for '{self.bronze_table_name}': {self.field_column_list}")

    def validate_columns(self, dataframe):
        """
        Validate that the required columns exist in the DataFrame.

        :param dataframe: DataFrame to validate.
        :return: List of missing columns.
        """
        missing_columns = [col for col in self.field_column_list if col not in dataframe.columns]
        if missing_columns:
            self.logger.warning(f"Missing columns in DataFrame: {missing_columns}")
        else:
            self.logger.info("All required columns are present in the DataFrame.")
        return missing_columns

    @abstractmethod
    def validate(self):
        """
        Abstract method to perform file-specific validation logic.
        Must be implemented in derived classes.
        """
        pass

    @abstractmethod
    def process(self):
        """
        Abstract method to process the file data.
        Must be implemented in derived classes.
        """
        pass
