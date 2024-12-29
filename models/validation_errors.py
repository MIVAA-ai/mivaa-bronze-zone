from utils.db_util import con
import pandas as pd
from datetime import datetime


def log_errors_to_db(errors: list, file_id: int):
    """
    Log validation errors to the database.
    """
    # Ensure the table exists
    con.execute("""
        CREATE TABLE IF NOT EXISTS validation_errors (
            error_id INTEGER PRIMARY KEY,
            file_id INTEGER,
            row_index INTEGER,
            field_name TEXT,
            error_type TEXT,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Convert errors to a DataFrame
    error_df = pd.DataFrame(errors)

    # Handle empty DataFrame case
    if error_df.empty:
        print("No errors to log.")
        return

    # Add required fields
    max_id = con.execute("SELECT MAX(error_id) FROM validation_errors").fetchone()[0] or 0
    error_df["error_id"] = range(max_id + 1, max_id + 1 + len(error_df))
    error_df["file_id"] = file_id
    error_df["created_at"] = datetime.now()

    # Ensure the column order matches the table schema
    column_order = [
        "error_id", "file_id", "row_index", "field_name", "error_type", "error_message",
        "error_message", "created_at"
    ]
    error_df = error_df[column_order]

    # Insert into the database
    con.register("error_temp", error_df)
    print('******')
    pd.set_option('display.max_columns', None)
    print(error_df)
    con.execute("""
        INSERT INTO validation_errors 
        SELECT error_id, file_id, row_index, field_name, error_type, error_message, created_at 
        FROM error_temp
    """)
    con.unregister("error_temp")

    print("Errors logged successfully.")
