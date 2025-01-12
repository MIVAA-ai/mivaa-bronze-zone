import json
import os

from config.logger_config import configure_logger
from sqlalchemy import text

from models.sql_script_store import SQLScriptStore
from utils.db_util import get_session, engine

logger = configure_logger(__name__)

# Path to the JSON schema file
JSON_FILE_PATH = "config/schema.json"

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

if __name__ == "__main__":
    """
    Main entry point for executing the database initialization script.
    """
    try:
        logger.info("Initializing database from JSON Schema file.")
        initialize_database_from_json()
        logger.info("Database initialization completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred during database initialization: {e}")

    logger.info("Database initialized. Starting the application...")
    # Import and start the application
    from app import start_app

    start_app()