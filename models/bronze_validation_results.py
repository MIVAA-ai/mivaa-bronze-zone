from utils.db_util import con
import pandas as pd
from datetime import datetime

def log_field_bronze_table(df: pd.DataFrame, file_id: int, error_index_set):
    # Ensure the table exists
    con.execute("""
        CREATE TABLE IF NOT EXISTS field_bronze_table (
            id INTEGER PRIMARY KEY,  -- Unique ID for each record
            row_index INTEGER NOT NULL,
            file_id INTEGER NOT NULL,              -- ID of the file being validated
            validation_status TEXT NOT NULL,       -- Status of validation (e.g., Passed, Failed)
            FieldName TEXT,                        -- Name of the field being validated
            FieldType TEXT,                        -- Type of the field (e.g., string, integer)
            DiscoveryDate TIMESTAMP,               -- Discovery date of the field
            X REAL,                                -- X-coordinate for spatial data
            Y REAL,                                -- Y-coordinate for spatial data
            CRS TEXT,                              -- Coordinate Reference System
            Source TEXT,                           -- Source of the data
            ParentFieldName TEXT,                  -- Parent field name, if applicable
            validation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Timestamp of validation
        );
    """)

    """
    Log validation status for each row in the database.
    """
    # Add file_id and validation status
    max_id = con.execute("SELECT MAX(id) FROM field_bronze_table").fetchone()[0] or 0
    df["id"] = range(max_id + 1, max_id + 1 + len(df))
    df["row_index"] = df.index
    df["file_id"] = file_id
    # Assign "Passed" or "Failed" based on whether the index is in the error set
    df["validation_status"] = ["Failed" if idx in error_index_set else "Passed" for idx in df.index]
    df["validation_timestamp"] = datetime.now()

    # Ensure the column order matches the table schema
    column_order = [
        "id", "row_index", "file_id", "validation_status", "FieldName", "FieldType", "DiscoveryDate",
        "X", "Y", "CRS", "Source", "ParentFieldName", "validation_timestamp"
    ]
    df = df[column_order]

    # Insert validation results
    con.register("validation_temp", df)
    print(con.execute("""SELECT * FROM validation_temp""").fetchone())
    con.execute("""
        INSERT INTO field_bronze_table SELECT * FROM validation_temp
    """)
    con.unregister("validation_temp")

    print("Validation results logged successfully.")

