from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 
url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
def log_progress(message):
    time_format= "%Y-%h-%d-%H:%M:%S"
    now=datetime.now()
    timestamp=now.strftime(time_format)
    with open('code_log.txt', 'a') as f:
        f.write(timestamp+ ':' + message + '\n')

def extract(url, table_attribs):
    page = requests.get(url).text
    soup = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = soup.find_all('table', {'class': 'wikitable'})    
    rows=tables[0].find_all('tr')
    for row in rows:
        col=row.find_all('td')
        if len(col)!=0:
            data_dict={'Name':col[1].find_all('a')[1],
                       'MC_USD_Billion':col[2].contents[0]}
            dataframe=pd.DataFrame(data_dict, index=[0])
            df=pd.concat([df,dataframe], ignore_index=True)
    df['MC_USD_Billion'] = df['MC_USD_Billion'].str.replace('\n', '').astype(float)
    return df

log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)
print(df)

def transform(df):
    data=pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv')
    dict = data.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*dict['INR'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*dict['EUR'],2) for x in df['MC_USD_Billion']]
    return df
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df)
print("the market capitalization of the 5th largest bank in billion EUR is :" ,df['MC_EUR_Billion'][4])

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)
load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index= False)
sql_connection=sqlite3.connect(db_name)
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')


def run_queries(query_statement, sql_connection):
    query_output=pd.read_sql(query_statement,sql_connection)
    print(query_statement)
    print(query_output)

query_statement=f"SELECT * FROM Largest_banks"
run_queries(query_statement,sql_connection)
query_statement2=f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_queries(query_statement2,sql_connection)
query_statement3=f"SELECT Name from Largest_banks LIMIT 5"
run_queries(query_statement3,sql_connection)
log_progress('Process Complete.')
sql_connection.close()





