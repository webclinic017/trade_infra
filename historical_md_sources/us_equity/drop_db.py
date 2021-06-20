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


# cursor.execute("DROP TABLE us_equity_symbols")
# cursor.execute("DROP TABLE us_equity_minute_price")
# cursor.execute("DROP TABLE us_equity_daily_price")

# cursor.execute("DROP TABLE md_stream")

cursor.execute("DELETE FROM md_stream") #clean the table everyday

# sql = "INSERT INTO md_stream (symbol, datetime, open, high, low, close, volume, notes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
# values = ('MSFT', '2021-01-11 09:24:00', 217.5, 217.85, 217.3, 217.7, 4587, None)
# cursor.execute(sql, values)

connection.commit()
cursor.close()
connection.close()