import duckdb
con = duckdb.connect('my_database.db')

def truncate_table(table_name):

        # Construct and execute the DELETE query
        query = f"DELETE FROM {table_name};"
        con.execute(query)

        print(f"Successfully truncated table: {table_name}")

def fetch_data_conditionaly(table_name,row_identifier_column,row_identifier_value):
        sql_query = f"""
        select * FROM {table_name}
        WHERE {row_identifier_column} = ?
        """

        return con.execute(sql_query, (row_identifier_value,)).fetchdf()
