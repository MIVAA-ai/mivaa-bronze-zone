from sqlalchemy import Column, Integer, Text, Float, TIMESTAMP, text
from sqlalchemy.ext.declarative import declarative_base
import re

from config.logger_config import configure_logger
from utils.db_util import get_session

# Configure logger
logger = configure_logger("model_generation.log")

# Initialize SQLAlchemy base class
Base = declarative_base()

# Map SQL types to SQLAlchemy types
SQL_TYPE_MAP = {
    "INTEGER": Integer,
    "TEXT": Text,
    "REAL": Float,
    "TIMESTAMP": TIMESTAMP,
    "STRING": Text,  # Default to Text for variable-length text columns
}

def get_create_schema_from_db(table_name):
    """
    Query the `sql_script_store` table to retrieve the CREATE TABLE schema for the given table.

    :param table_name: The name of the table to get the schema for.
    :return: The CREATE TABLE SQL statement.
    """
    with get_session() as session:
        try:
            result = session.execute(
                text("SELECT query FROM sql_script_store WHERE table_name = :table_name"),
                {"table_name": table_name}
            ).fetchone()

            if not result:
                logger.error(f"No schema found for table: {table_name}")
                raise ValueError(f"No schema found for table: {table_name}")

            logger.info(f"Schema retrieved for table '{table_name}': {result[0]}")
            return result[0]
        except Exception as e:
            logger.error(f"Error retrieving schema for table '{table_name}': {e}")
            raise

def parse_create_table_sql(sql):
    """
    Parse a CREATE TABLE SQL statement to extract the table name and columns.

    :param sql: The CREATE TABLE SQL statement as a string.
    :return: A tuple containing the table name and a list of column definitions.
    """
    try:
        table_name_match = re.search(r"CREATE TABLE IF NOT EXISTS\s+(\w+)", sql, re.IGNORECASE)
        if not table_name_match:
            logger.error("Could not extract table name from SQL")
            raise ValueError("Could not extract table name from SQL")

        table_name = table_name_match.group(1)

        # Extract column definitions
        column_defs_match = re.search(r"\((.+)\)", sql, re.DOTALL)
        if not column_defs_match:
            logger.error("Could not extract column definitions from SQL")
            raise ValueError("Could not extract column definitions from SQL")

        column_defs = column_defs_match.group(1).split(",")
        columns = []

        for column_def in column_defs:
            parts = column_def.strip().split()
            column_name = parts[0]
            column_type = parts[1]
            nullable = not ("NOT NULL" in column_def.upper())
            primary_key = "PRIMARY KEY" in column_def.upper()

            columns.append({
                "name": column_name,
                "type": column_type,
                "nullable": nullable,
                "primary_key": primary_key
            })

        logger.info(f"Parsed table '{table_name}' with columns: {columns}")
        return table_name, columns
    except Exception as e:
        logger.error(f"Error parsing CREATE TABLE SQL: {e}")
        raise

def generate_model_class(sql):
    """
    Generate an SQLAlchemy model class from a CREATE TABLE SQL statement.

    :param sql: The CREATE TABLE SQL statement as a string.
    :return: The generated SQLAlchemy model class.
    """
    try:
        table_name, columns = parse_create_table_sql(sql)

        # Dynamically create a dictionary for class attributes
        class_attributes = {"__tablename__": table_name}

        for column in columns:
            column_type = SQL_TYPE_MAP.get(column["type"].upper(), Text)  # Default to Text if type is unknown
            class_attributes[column["name"]] = Column(
                column_type,
                nullable=column["nullable"],
                primary_key=column["primary_key"]
            )
        print(class_attributes)
        # Dynamically create a new SQLAlchemy model class
        model_class = type(table_name.capitalize(), (Base,), class_attributes)
        logger.info(f"Model class generated for table '{table_name}'")

        return model_class
    except Exception as e:
        logger.error(f"Error generating model class: {e}")
        raise

def generate_model_for_table(table_name):
    """
    Generate an SQLAlchemy model class for a given table by querying the database.

    :param table_name: The name of the table to generate the model for.
    :return: The generated SQLAlchemy model class.
    """
    try:
        create_table_sql = get_create_schema_from_db(table_name)
        logger.info(f"Retrieved schema for table '{table_name}': {create_table_sql}")
        return generate_model_class(create_table_sql)
    except Exception as e:
        logger.error(f"Error generating model for table '{table_name}': {e}")
        raise
