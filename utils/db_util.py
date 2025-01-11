import hashlib
import logging
from sqlalchemy import create_engine, text, Column, String, Text, CheckConstraint, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.inspection import inspect
from contextlib import contextmanager
import os
import json
from config.logger_config import configure_logger

# Configure logger
logger = configure_logger("database_operations.log")

# Database connection URL (DuckDB)
DATABASE_URL = "duckdb:///my_database.db"

# Path to the JSON schema file
JSON_FILE_PATH = "config/schema.json"

# Initialize SQLAlchemy components
Base = declarative_base()

# Create the engine
engine = create_engine(DATABASE_URL)

# Create a configured "Session" class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@contextmanager
def get_session():
    """
    Provides a new SQLAlchemy session.
    Use this function with `with` statements to ensure proper resource management.
    """
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()

# Define the sql_script_store table
class SQLScriptStore(Base):
    __tablename__ = "sql_script_store"

    zone = Column(
        String,
        CheckConstraint("zone IN ('COMMON','BRONZE','SILVER','GOLD')"),
        nullable=False
    )
    query = Column(Text, nullable=False)
    query_type = Column(
        String,
        CheckConstraint("query_type IN ('SELECT','UPDATE','DELETE','CREATE','DROP','OTHER')"),
        nullable=False
    )
    table_name = Column(String, nullable=False)
    data_columns = Column(Text)  # Added data_columns to store the list of columns

    __table_args__ = (
        CheckConstraint("zone IN ('COMMON', 'BRONZE', 'SILVER', 'GOLD')"),
        CheckConstraint("query_type IN ('SELECT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'OTHER')"),
        PrimaryKeyConstraint("table_name", "query_type"),  # Define composite primary key here
    )

# Create the sql_script_store table
Base.metadata.create_all(bind=engine)

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

def initialize_database_from_json(json_file_path=JSON_FILE_PATH):
    """
    Executes all SQL statements from a JSON schema file to initialize the database.
    Stores table definitions into the sql_script_store table, including column lists.

    :param json_file_path: Path to the JSON file containing schema definitions.
    """
    if not os.path.exists(json_file_path):
        logger.error(f"JSON schema file not found: {json_file_path}")
        raise FileNotFoundError(f"JSON schema file not found: {json_file_path}")

    with open(json_file_path, "r") as file:
        schema_data = json.load(file)

    # Validate that the JSON is a list of objects with required keys
    required_keys = {"zone", "query", "query_type", "table_name"}
    for entry in schema_data:
        if not required_keys.issubset(entry.keys()):
            logger.error(f"Invalid JSON entry: {entry}. Required keys: {required_keys}")
            raise ValueError(f"Invalid JSON entry: {entry}. Required keys: {required_keys}")

    with get_session() as session:
        for entry in schema_data:
            query = entry["query"]
            try:
                # Execute the table creation query
                logger.info(f"Executing statement for table: {entry['table_name']} in zone: {entry['zone']}")
                session.execute(text(query))
                session.commit()

                if "data_columns" in entry:
                    data_columns_str = entry['data_columns']
                else:
                    data_columns_str = None

                # Insert the table definition into the sql_script_store table
                sql_script_entry = SQLScriptStore(
                    zone=entry["zone"],
                    query=query,
                    query_type=entry["query_type"],
                    table_name=entry["table_name"],
                    data_columns=data_columns_str
                )
                session.add(sql_script_entry)
                logger.info(f"Stored table definition for {entry['table_name']} with columns: {data_columns_str}.")
            except Exception as e:
                logger.error(f"Error executing statement for table {entry['table_name']}:{query}Error: {e}")
        session.commit()

        # Display tables in the database
        try:
            tables = session.execute(text("SHOW TABLES")).fetchall()
            logger.info("Tables in the database:")
            for table in tables:
                logger.info(f"- {table[0]}")
        except Exception as e:
            logger.error("Could not retrieve tables from the database:", e)

    logger.info("Database schema initialization complete.")
    with get_session() as connection:
        result = connection.execute(text("PRAGMA table_info('sql_script_store')"))
        for row in result.fetchall():
            logger.debug(row)
