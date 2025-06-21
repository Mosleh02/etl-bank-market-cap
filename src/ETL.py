import pandas
import requests
import bs4
import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
load_dotenv("C:\\Users\\User\Desktop\\Computer Science\\Projects\\etl-bank-market-cap\\env_vars.env")
db_password = os.environ.get("DB_PASSWORD")
db_user = os.environ.get("DB_USER")
db_host = os.environ.get("DB_HOST")
db_port = os.environ.get("DB_PORT")
db_name = os.environ.get("DB_NAME")



def log_progress(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("C:\\Users\\User\\Desktop\\Computer Science\\Projects\\etl-bank-market-cap\\logs\\code_log.txt", "a") as file:
        file.write(f"{timestamp} : {message}\n")



def extract(url):
    page = requests.get(url)
    src = page.content
    soup = bs4.BeautifulSoup(src,'html.parser')
    table = soup.find("table")

    table_attribs = []
    for i in table.find_all("th"):
        table_attribs.append(i.text.strip())
    df = pandas.DataFrame(columns=table_attribs)
    
    ranks = []
    for i in table.find_all('td'):
        ranks.append(i.text.strip())
    for i in range( 0 , len(ranks)-2 , 3 ):
        rank = ranks[i]
        bank_name = ranks[i+1]
        Market_cap = ranks[i+2]
        df.loc[len(df)] = [rank, bank_name, Market_cap]
    df['Market cap(US$ billion)'] = pandas.to_numeric(df['Market cap(US$ billion)'], errors='coerce')
    return df



def transform(df, rates_url):
    rates = pandas.read_csv(rates_url)
    rates['Rate'] = pandas.to_numeric(rates['Rate'], errors='coerce')
    df['Market cap(EUR billion)'] = df['Market cap(US$ billion)'] * rates['Rate'].iloc[0]
    df['Market cap(GBP billion)'] = df['Market cap(US$ billion)'] * rates['Rate'].iloc[1]
    df['Market cap(INR billion)'] = df['Market cap(US$ billion)'] * rates['Rate'].iloc[2]
    return df



def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)



def load_to_db(df, engine, table_name):
    df.to_sql(table_name, engine, if_exists='replace', index=False)




def run_query(query_statement, engine):

    with engine.connect() as connection:

        result = connection.execute(text(query_statement))

        for row in result:
            print(row)




if __name__ == "__main__":

    url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
    rates_url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
    csv_output_path = "C:\\Users\\User\\Desktop\\Computer Science\\Projects\\etl-bank-market-cap\\data\\final_view.csv"

    log_progress("Preliminaries complete. Initiating ETL process.")

    try:
        df = extract(url)
        log_progress("Data extraction complete. Initiating Transformation process.")
    except:
        log_progress("Data extraction failed")
        raise



    try:
        df = transform(df, rates_url)
        log_progress("Data transformation complete. Initiating Loading process.")
    except:
        log_progress("Data transformation failed")
        raise



    try:
        load_to_csv(df, csv_output_path)
        log_progress("Data saved to CSV file.")
    except:
        log_progress("CSV export failed")
        raise



    try:
        engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
        log_progress("SQL Connection initiated.")
    except:
        log_progress("Database connection failed")
        raise



    try:
        load_to_db(df, engine, "top10banks")
        log_progress("Data loaded to Database as a table, Executing queries")
    except:
        log_progress("Database load failed")
        raise



    try:
        print("\nTop 5 Banks:")
        run_query('SELECT "Bank name" FROM top10banks LIMIT 5', engine)
        log_progress("Process Complete")

        print("\nAverage Market Cap in GBP:")
        run_query('SELECT AVG("Market cap(GBP billion)") FROM top10banks', engine)
        log_progress("Process Complete")

        print("\nFull Table Preview:")
        run_query('SELECT * FROM top10banks', engine)
        log_progress("Process Complete")
    except:
        log_progress("Query execution failed")
        raise
