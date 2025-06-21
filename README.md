# 📊 ETL Pipeline: Top 10 Banks by Market Capitalization

A Python-based ETL pipeline that extracts data from a static Wikipedia snapshot, transforms it by converting currency values using live exchange rates, and loads the results into a PostgreSQL database and a CSV file. The project includes logging and SQL querying for insights.

---

## 🔧 Features

- **Web Scraping**: Scrapes a table of the world’s top 10 banks by market capitalization from a Wikipedia snapshot using `requests` and `BeautifulSoup`.
- **Data Transformation**: Uses a live exchange rate CSV to convert market capitalization from USD to EUR, GBP, and INR using `pandas`.
- **Data Storage**: Saves the transformed data to both a local CSV file and a PostgreSQL database using `SQLAlchemy`.
- **Querying**: Executes SQL queries to extract specific insights from the data (e.g., top banks, average market cap).
- **Secure Credentials**: Uses `.env` files and `python-dotenv` to securely manage database credentials.
- **Logging**: Tracks progress and errors in a local log file.



## 📁 Project Structure

```
etl-bank-market-cap/
│
├── data/ # Optional: contains input/output CSVs
│ └── exchange_rate.csv
| └── final_view_py
│
├── logs/ # Contains log file
│ └── code_log.txt
│
├── src/ # Source code
│ └── ETL.py
│
├── .env.example # Template for environment variables
├── .gitignore
├── requirements.txt
├── README.md
└── LICENSE
```
## Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/Mosleh02/etl-bank-market-cap.git
cd etl-bank-market-cap
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables
```env
DB_USER=your_postgres_user
DB_PASSWORD=your_postgres_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database_name
```


### 4. Run the pipeline
```bash
python src/ETL.py
```


## Example outputs

✔️ CSV Output:
A file named final_view_py.csv is generated with market caps in USD, EUR, GBP, and INR.


✔️ SQL Output:
Sample queries executed by the script:

List top 5 banks by name

Average market cap in GBP

Full table preview



## Tech Stack

| Category      | Tools Used                  |
| ------------- | --------------------------- |
| Language      | Python 3                    |
| Web Scraping  | `requests`, `BeautifulSoup` |
| Data Handling | `pandas`, `numpy`           |
| Database      | PostgreSQL, `SQLAlchemy`    |
| Environment   | `.env`, `python-dotenv`     |
| Logging       | `datetime`, file I/O        |


## Notes
The Wikipedia source is from the [Wayback Machine](https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks) to ensure consistency over time.

Exchange rates are fetched from a static CSV hosted in IBM's Coursera cloud repository.

## License
This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.


## Author
Abdulrahman Mosleh  
[LinkedIn](https://www.linkedin.com/in/abdulrahman-mosleh-5a3147257) | [GitHub](https://github.com/Mosleh02)

