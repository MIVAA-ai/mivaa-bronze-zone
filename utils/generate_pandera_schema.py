from sqlalchemy import text

from config.logger_config import configure_logger
from utils.db_util import get_session

# Configure logger
logger = configure_logger("table_info.log")

# Pandera type mappings
TYPE_MAPPING = {
    "TEXT": "str",
    "VARCHAR": "str",
    "INTEGER": "int",
    "BIGINT": "int",
    "SMALLINT": "int",
    "REAL": "float",
    "DOUBLE": "float",
    "FLOAT": "float",
    "NUMERIC": "float",
    "DECIMAL": "float",
    "BOOLEAN": "bool",
    "TIMESTAMP": "pd.Timestamp",
    "DATE": "pd.Timestamp",
}

def fetch_data_columns(table_name):
    """
    Fetch the 'data_columns' field for the specified table from the sql_script_store table.

    :param table_name: Name of the table to fetch data_columns for.
    :return: List of column names or None if not found.
    """
    with get_session() as connection:
        try:
            result = connection.execute(
                text('SELECT "data_columns" FROM sql_script_store WHERE table_name = :table_name'),
                {"table_name": table_name}
            ).fetchone()
            if result and result[0]:
                return result[0].split(",")  # Convert the comma-separated string to a list
            else:
                logger.info(f"No data_columns found for table '{table_name}'.")
                return None
        except Exception as e:
            logger.error(f"Error fetching data_columns for table '{table_name}': {e}")
            return None

def fetch_table_info(table_name):
    """
    Fetch the table schema using PRAGMA table_info('{table_name}').

    :param table_name: The name of the table to inspect.
    :return: List of column info as tuples.
    """
    with get_session() as connection:
        try:
            result = connection.execute(text(f"PRAGMA table_info('{table_name}')")).fetchall()
            return result
        except Exception as e:
            logger.error(f"Error fetching table info for table '{table_name}': {e}")
            return None

def generate_pandera_class_from_table_info(table_name, class_name="GeneratedDataFrameModel"):
    """
    Generates a Pandera DataFrameModel class from a table's schema.

    :param table_name: The name of the table to describe.
    :param class_name: The name of the Pandera class to generate.
    :return: A string representation of the Pandera DataFrameModel class.
    """
    # Fetch table schema information
    table_info = fetch_table_info(table_name)

    # Fetch data_columns from sql_script_store
    data_columns = fetch_data_columns(table_name)

    if not table_info:
        logger.warning(f"No schema found for table '{table_name}'. Please ensure the table exists.")
        return None
    if not data_columns:
        logger.warning(f"No 'data_columns' metadata found for table '{table_name}' in sql_script_store.")

    # Generate the Pandera class
    class_lines = [f"class {class_name}(pa.DataFrameModel):"]
    class_lines.append("    # Define schema for the input data")
    for column in table_info:
        col_name = column[1]  # Column name
        col_type = column[2].upper()  # Data type (e.g., INTEGER, TEXT)
        not_null = column[3]  # NOT NULL constraint (1 if NOT NULL, else 0)

        if col_name in data_columns:
            # Map SQL type to Pandera type
            pandera_type = TYPE_MAPPING.get(col_type, "str")  # Default to str if type not mapped
            nullable = "False" if not_null else "True"
            class_lines.append(f"    {col_name}: Series[{pandera_type}] = pa.Field(nullable={nullable}, coerce=True)")

    logger.info(f"Generated Pandera DataFrameModel class for table '{table_name}'.")
    return "\n".join(class_lines)
