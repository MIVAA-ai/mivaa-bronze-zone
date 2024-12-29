import duckdb
from utils.db_util import con
import os
from utils.checksum_util import calculate_checksum
# Create the table with an auto-incrementing id (if it doesn't exist)
create_table_query = """
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY,  -- Automatically NOT NULL for PRIMARY KEY
    filename TEXT NOT NULL,  -- Ensure filename cannot be NULL
    filepath TEXT NOT NULL,  -- Ensure filepath cannot be NULL
    datatype TEXT NOT NULL,  -- Ensure datatype cannot be NULL
    checksum TEXT NOT NULL,  -- Ensure checksum cannot be NULL
    remarks TEXT,            -- Remarks can be NULL
    status TEXT              -- Status can be NULL
);
"""
"""
    Status
    1: picked
    2: Processing
    3: processed 
    4: Error

"""

con.execute(create_table_query)
print("Table 'files' created successfully.")


# Function to insert data into the table
def insert_data(con, filepath, datatype, remarks):
    # Extract filename from filepath
    filename = os.path.basename(filepath)
    # Generate checksum using the imported function
    checksum = calculate_checksum(filepath)

    try:
        last_id = con.execute("SELECT MAX(id) FROM files").fetchone()[0] or 0
    except duckdb.CatalogException:
        last_id = 0  # Table is empty

    # Insert data
    insert_query = """
    INSERT INTO files (id, filename, filepath, datatype, checksum, remarks, status)
    VALUES (?, ?, ?, ?, ?, ?, ?);
    """
    new_id = last_id + 1  # Increment ID manually
    con.execute(insert_query, [new_id, filename, filepath, datatype, checksum, remarks, '1'])
    print(f"Inserted: {filename} with checksum {checksum}")
    return new_id


def fetch_files_to_process(con):
    return con.execute("SELECT * FROM files where status = '1'").fetchone()


def update_file_status(status, id, remarks=None):
    if remarks is not None:
        sql_query = f"""
            UPDATE files
            SET status = ?, remarks = ?
            WHERE id = ?"""
        con.execute(sql_query, (status, remarks, id))
    else:
        sql_query = f"""
            UPDATE files
            SET status = ?
            WHERE id = ?"""
        con.execute(sql_query, (status, id))





