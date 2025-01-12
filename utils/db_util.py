from sqlalchemy import create_engine, text, Column, String, Text, CheckConstraint, PrimaryKeyConstraint

from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from config.logger_config import configure_logger
from models.sql_script_store import SQLScriptStore

# Configure logger
logger = configure_logger("database_operations.log")

# Database connection URL (DuckDB)
DATABASE_URL = "duckdb:///my_database.db"

# Create the engine
engine = create_engine(DATABASE_URL)

# Create a configured "Session" class
SessionLocal = sessionmaker(autobegin=True, autoflush=False, bind=engine)

@contextmanager
def get_session():
    """
    Provides a transactional scope for database operations.
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()  # Explicitly commit the transaction
    except Exception as e:
        session.rollback()  # Rollback on error
        raise e
    finally:
        session.close()


def get_columns_from_store(table_name):
    """
    Fetch the list of columns for a specific table from the sql_script_store table.

    :param table_name: Name of the table to fetch the column list for.
    :return: List of column names or None if not found.
    """
    with get_session() as session:
        try:
            result = session.query(SQLScriptStore.data_columns).filter(SQLScriptStore.table_name == table_name).first()
            if result and result.data_columns:
                return result.data_columns.split(",")  # Convert the comma-separated string to a list
            else:
                logger.info(f"No column list found for table '{table_name}'.")
                return None
        except Exception as e:
            logger.error(f"Error fetching columns for table '{table_name}': {e}")
            return None