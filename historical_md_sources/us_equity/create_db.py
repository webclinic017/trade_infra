import sys
sys.path.append("/Users/led/Desktop/algo_trading_research/aggregation")

import psycopg2, config

#connect to the db
connection = psycopg2.connect(
    host = config.PSQL_HOST,
    port = config.PSQL_PORT,
    database = config.PSQL_DATABASE,
    user = config.PSQL_USER,
    password = config.PSQL_PASSWORD
)

cursor = connection.cursor()

#create stock ticker symbols reference table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS us_equity_symbols (
        id SERIAL PRIMARY KEY, 
        symbol TEXT NOT NULL UNIQUE, 
        name TEXT NOT NULL,
        exchange TEXT NOT NULL,
        shortable BOOLEAN NOT NULL,
        fractionable BOOLEAN NOT NULL, 
        marginable BOOLEAN NOT NULL
    )
""")

#create one table for all stocks day prices
cursor.execute("""
    CREATE TABLE IF NOT EXISTS us_equity_daily_price (
        id SERIAL PRIMARY KEY, 
        stock_id SERIAL,
        date DATE NOT NULL,
        open FLOAT NOT NULL, 
        high FLOAT NOT NULL, 
        low FLOAT NOT NULL, 
        close FLOAT NOT NULL, 
        volume INTEGER NOT NULL,
        FOREIGN KEY (stock_id) REFERENCES stock_symbols (id)
    )
""")

#create one table for all stocks minute prices
cursor.execute("""
    CREATE TABLE IF NOT EXISTS us_equity_minute_price (
        id SERIAL PRIMARY KEY, 
        stock_id SERIAL,
        datetime TIMESTAMP NOT NULL,
        open FLOAT NOT NULL, 
        high FLOAT NOT NULL, 
        low FLOAT NOT NULL, 
        close FLOAT NOT NULL, 
        volume INTEGER NOT NULL,
        FOREIGN KEY (stock_id) REFERENCES stock_symbols (id)
    )
""")


# minute, open, high, low, close, volume, notes
cursor.execute("""
    CREATE TABLE IF NOT EXISTS md_stream (
        row_id SERIAL PRIMARY KEY, 
        symbol TEXT NOT NULL,
        datetime TIMESTAMP NOT NULL,
        open FLOAT NOT NULL, 
        high FLOAT NOT NULL, 
        low FLOAT NOT NULL, 
        close FLOAT NOT NULL, 
        volume INTEGER NOT NULL,
        notes TEXT
    )
""")

connection.commit()
cursor.close()
connection.close()
print ('create_db completed')