from crawler import start_polling_thread, poll_table
from validator.field_validator import validate_field, field_column_list
from utils.db_util import con
from models.files import insert_data, fetch_files_to_process, update_file_status
import pandas as pd
def insert_fields_data_in_db(filepath):
    insert_data(con, str(filepath), 'field', '')

def validate_columns(df):
    # Check if all columns in the list exist in the DataFrame
    missing_columns = [col for col in field_column_list if col not in df.columns]
    return len(missing_columns)

def read_fields_data_in_db():
    results = fetch_files_to_process(con)
    print("Files to process:")
    # Set option to display all columns
    pd.set_option('display.max_columns', None)
    print(results)
    if results is not None:
        # Fetch the file to process
        df = pd.read_csv(results[2])
        if validate_columns(df):
            update_file_status('4', results[0],"Error: Columns does not match")
        else:
            update_file_status('2', results[0])
            validate_field(df, results[0], results[1])
            update_file_status('3', results[0])


if __name__ == "__main__":
    start_polling_thread(insert_fields_data_in_db)
    poll_table(read_fields_data_in_db)
