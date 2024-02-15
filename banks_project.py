import requests
from bs4 import BeautifulSoup
import pandas as pd 
import numpy as np 
import sqlite3
from datetime import datetime 

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path = '/home/project/exchange_rate.csv'
exchange_rate = pd.read_csv(csv_path)
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
table_attribs = ["Name", "MC_USD_Billion"]

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the 
    code execution to a log file. Function returns nothing.'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')   


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            #if col[0].find('a') is not None:
            data_dict = {"Name": str(col[1].get_text(strip=True)),
                            "MC_USD_Billion": float(col[2].get_text(strip=True))}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df


def transform(df, exchange_rate):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
   
    
    exchange_rates = exchange_rate.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rates['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rates['EUR'],2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rates['INR'],2)

    #print(df['MC_EUR_Billion'][4])

    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(csv_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)







def run_query(query_statement, sql_connection):
 
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)















        

log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url,table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df, exchange_rate)
log_progress('Data transformation complete. Initiating Loading process')
load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect('Banks.db')
log_progress('SQL Connection initiated')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')
query_statement = f"SELECT * from Largest_banks"
run_query(query_statement, sql_connection)
query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)
query_statement = f"SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)
log_progress('Process Complete')
log_progress('Server Connection closed')
sql_connection.close()