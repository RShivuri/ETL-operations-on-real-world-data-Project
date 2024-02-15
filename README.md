# ETL-operations-on-real-world-data-Project
ETL operations on real-world data Project
This Python script performs an ETL (Extract, Transform, Load) process on bank data extracted from a Wikipedia page. Below is the documentation for each function:

1. **`log_progress(message)`**:
   - Description: Logs messages with timestamps to a log file to track the progress of the ETL process.
   - Parameters:
     - `message`: A string representing the progress message to be logged.
   - Returns: None.

2. **`extract(url, table_attribs)`**:
   - Description: Extracts data from a specified Wikipedia URL and constructs a DataFrame containing bank names and market capitalization in USD billion.
   - Parameters:
     - `url`: The URL of the Wikipedia page from which data is to be extracted.
     - `table_attribs`: A list of column names for the DataFrame.
   - Returns: A DataFrame containing extracted data.

3. **`transform(df, exchange_rate)`**:
   - Description: Transforms the extracted DataFrame by converting market capitalization from USD to GBP, EUR, and INR.
   - Parameters:
     - `df`: The DataFrame containing the extracted data.
     - `exchange_rate`: DataFrame containing exchange rates for different currencies.
   - Returns: The transformed DataFrame.

4. **`load_to_csv(df, csv_path)`**:
   - Description: Saves the final DataFrame to a CSV file at the specified path.
   - Parameters:
     - `df`: The DataFrame to be saved.
     - `csv_path`: The path where the CSV file will be saved.
   - Returns: None.

5. **`load_to_db(df, sql_connection, table_name)`**:
   - Description: Saves the final DataFrame to a SQLite database table with the specified name.
   - Parameters:
     - `df`: The DataFrame to be saved.
     - `sql_connection`: SQLite database connection.
     - `table_name`: Name of the table to which the DataFrame will be saved.
   - Returns: None.

6. **`run_query(query_statement, sql_connection)`**:
   - Description: Executes SQL queries on the connected SQLite database.
   - Parameters:
     - `query_statement`: SQL query to be executed.
     - `sql_connection`: SQLite database connection.
   - Returns: None.

The script begins by logging the start of the ETL process, then extracts data from the Wikipedia page, transforms it, and saves it to both a CSV file and a SQLite database. Finally, it executes some SQL queries on the database and logs the completion of the process.
