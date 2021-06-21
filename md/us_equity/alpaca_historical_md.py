from pathlib import Path
infra_path = Path(__file__).resolve().parents[2]  #this will return infra folder

import sys
if "infra" in str(infra_path):
    sys.path.append(str(infra_path))
else:
    raise Exception("Infra parent folder is not able to append, check parent directory hierarchy level")

import alpaca_trade_api as tradeapi
import psycopg2, config
from datetime import date, datetime, timedelta
import pandas as pd
from spy_tickers import spy_tickers

import dateutil.parser as parser

# {   'class': 'us_equity',
#     'easy_to_borrow': False,
#     'exchange': 'NASDAQ',
#     'fractionable': False,
#     'id': 'fba7219f-94cb-40e4-a74f-fb42508ff55b',
#     'marginable': True,
#     'name': 'Protagenic Therapeutics, Inc. Common Stock',
#     'shortable': False,
#     'status': 'active',
#     'symbol': 'PTIX',
#     'tradable': True})

#connect to the db
connection = psycopg2.connect(
    host = config.PSQL_HOST,
    port = config.PSQL_PORT,
    database = config.PSQL_DATABASE,
    user = config.PSQL_USER,
    password = config.PSQL_PASSWORD
)
cursor = connection.cursor()

api = tradeapi.REST(
    base_url = config.ALPCA_PAPER_ENDPOINT,
    key_id = config.ALPACA_PAPER_API_KEY,
    secret_key= config.ALPACA_ALPACA_SECRET_KEY
)

def download_symbols():
    cursor.execute("""
        SELECT symbol, name FROM us_equity_symbols
    """)
    rows = cursor.fetchall()
    symbols = [row[0] for row in rows]
    # print (symbols)

    assets = api.list_assets()

    id_key = 0
    for asset in assets:
        id_key += 1
        try:
            if asset.status == 'active' and asset.tradable and asset.symbol not in symbols:  #only add symbol not in current db. 
                print(f"Added a new stock {asset.symbol} {asset.name}")
                # print(id_key, asset.symbol, asset.name, asset.exchange, asset.shortable)
                cursor.execute("INSERT INTO us_equity_symbols (id, symbol, name, exchange, shortable, fractionable, marginable) VALUES \
                                (%s, %s, %s, %s, %s, %s, %s)", \
                                (id_key, asset.symbol, asset.name, asset.exchange, asset.shortable, asset.fractionable, asset.marginable))

        except Exception as e:
            print(asset.symbol)
            print(e)

    connection.commit()
    print ('populate_db completed')

# download_symbols()


def existing_db_daily_data():
    existing_db_daily_data = []

    cursor.execute("""
        SELECT * FROM us_equity_daily_price; 
    """)
    current_daily_price_db_data = cursor.fetchall()

    for current_daily in current_daily_price_db_data:
        db_ticker_index = current_daily[1]

        db_date = current_daily[2]
        my_time = datetime.min.time()
        my_datetime = datetime.combine(db_date, my_time)
        my_datetime = my_datetime.isoformat()

        existing_data = str(db_ticker_index) + str(my_datetime)
        existing_db_daily_data.append(existing_data)

    return existing_db_daily_data

# existing_db_daily_data()


def populate_daily_data():
    NY = 'America/New_York'
    symbols = []
    stock_ids = {}
    spy = spy_tickers()

    cursor.execute("""
        SELECT * FROM us_equity_symbols; 
    """)
    stocks = cursor.fetchall()

    for stock in stocks:
        symbol = stock[1]
        if symbol in spy:   #Only want SPY tickers
            symbols.append(symbol)
            stock_ids[symbol] = stock[0]

    for symbol in symbols:
        start_date = datetime(2010, 1, 4).date()  #this date has to be monday, because +4 days =  full 5 week days. 
        end_date_range = date.today()

        while start_date < end_date_range:
            end_date = start_date + timedelta(days=4)

            print(f"== Fetching day bars for {symbol} {start_date} - {end_date} ==")
            minutes = api.get_barset(symbol,'day', start=pd.Timestamp(start_date, tz=NY).isoformat(), end=pd.Timestamp(end_date, tz=NY).isoformat()).df  #some symbol may have no data for some period
            minutes = minutes.resample('1d').ffill()
            # print (minutes)

            for index, row in minutes.iterrows():

                download_date = index.tz_localize(None).isoformat()
                if str(stock_ids[symbol]) + str(download_date) not in existing_db_daily_data():  #check if already in the database. base on stock_id+date index key.
                    print (f"Added new bar for {symbol}  {download_date}")
                    cursor.execute("""
                        INSERT INTO us_equity_daily_price (stock_id, date, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (stock_ids[symbol], download_date, float(row[0]), float(row[1]), float(row[2]), float(row[3]), int(row[4])))
                    # """, (stock_ids[symbol], download_date, row['open'], row['high'], row['low'], row['close'], row['volume']))
                else:
                    pass

            start_date = start_date + timedelta(days=7)
            
    connection.commit()

populate_daily_data()

def populate_minute_data():
    symbols = []
    stock_ids = {}
    spy = spy_tickers()

    cursor.execute("""
        SELECT * FROM stock_symbols; 
    """)
    stocks = cursor.fetchall()

    for stock in stocks:
        symbol = stock[1]
        if symbol in spy:   #Only want SPY tickers
            symbols.append(symbol)
            stock_ids[symbol] = stock[0]

    for symbol in symbols:
        start_date = datetime(2010, 1, 4).date()
        end_date_range = date.today()

        while start_date < end_date_range:
            end_date = start_date + timedelta(days=4)

            print(f"== Fetching day bars for {symbol} {start_date} - {end_date} ==")
            minutes = api.get_barset(symbol,'minute', start=pd.Timestamp(start_date, tz=NY).isoformat(), end=pd.Timestamp(end_date, tz=NY).isoformat()).df  #some symbol may have no data for some period
            minutes = minutes.resample('1m').ffill()
            # print (minutes)

            for index, row in minutes.iterrows():

                download_date = index.tz_localize(None).isoformat()
                if str(stock_ids[symbol]) + str(download_date) not in existing_db_daily_data():  #check if already in the database. base on stock_id+date index key.
                    print (f"Added new bar for {symbol}  {download_date}")
                    cursor.execute("""
                        INSERT INTO us_equity_daily_price (stock_id, datetime, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (stock_ids[symbol], download_date, float(row[0]), float(row[1]), float(row[2]), float(row[3]), int(row[4])))
                    # """, (stock_ids[symbol], download_date, row['open'], row['high'], row['low'], row['close'], row['volume']))
                else:
                    pass

            start_date = start_date + timedelta(days=7)

        # while start_date < end_date_range:
        #     end_date = start_date + timedelta(days=4)

        #     print(f"== Fetching minute bars for {symbol} {start_date} - {end_date} ==")
        #     minutes = api.polygon.historic_agg_v2(symbol, 1, 'minute', _from=start_date, to=end_date).df
        #     minutes = minutes.resample('1min').ffill()

        #     for index, row in minutes.iterrows():
        #         cursor.execute("""
        #             INSERT INTO us_equity_minute_price (stock_id, datetime, open, high, low, close, volume)
        #             VALUES (%s, %s, %s, %s, %s, %s, %s)
        #         """, (stock_ids[symbol], index.tz_localize(None).isoformat(), row['open'], row['high'], row['low'], row['close'], row['volume']))

        #     start_date = start_date + timedelta(days=7)
            
    connection.commit()

# print (populate_minute_data())

cursor.close()
connection.close()
