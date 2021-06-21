import sys
sys.path.append("/Users/led/Desktop/algo_trading_research/aggregation")

import config, psycopg2
import websocket, json
import alpaca_trade_api as tradeapi
from datetime import datetime

#connect to the db
connection = psycopg2.connect(
    host = config.PSQL_HOST,
    port = config.PSQL_PORT,
    database = config.PSQL_DATABASE,
    user = config.PSQL_USER,
    password = config.PSQL_PASSWORD
)
cursor = connection.cursor()

alp_api = tradeapi.REST(
    base_url = config.ALPACA_BASE_PAPER_URL,
    key_id = config.PAPER_API_KEY,
    secret_key= config.PAPER_SECRET_KEY
)

tickers = config.md_tickers

subscribe_tickers = ["AM." + i for i in tickers]  #add AM to each ticker in tickers list
subscribe_tickers = ",".join(str(e) for e in subscribe_tickers)  #Output: "AM.BYND,AM.MSFT,AM.GOOG,AM.AAPL,AM.SH,AM.SQQQ,AM.SPXL"

print ('Listening to: ' + str(tickers))

def on_open(ws):  #subscribe and listen data
    print ("opened")
    auth_data = {
        "action": "auth",
        "params": config.PAPER_API_KEY
    }
    ws.send(json.dumps(auth_data))
    channel_data = {
        "action": "subscribe",
        "params": subscribe_tickers
    }
    ws.send(json.dumps(channel_data))  #send request in json format to websocket

def on_message(ws, message):
    current_ticks = json.loads(message)
    print ("current_tick:", current_ticks)

    for current_tick in current_ticks:  #sometimes, two ticker come with one call, most of the time, they come seperate.
        current_tick_dict = current_tick
        tick_open_time = datetime.fromtimestamp(current_tick_dict.get('s')/1000).strftime('%Y-%m-%d %H:%M:00')
        current_tick_dict.update({'s': tick_open_time})

        tick_close_time = datetime.fromtimestamp(current_tick_dict.get('e')/1000).strftime('%Y-%m-%d %H:%M:00')
        current_tick_dict.update({'e': tick_close_time})

        for i in tickers:
            # print ("printing i", i, "current ticker:", current_tick_dict['sym'])
            if current_tick_dict['sym'] == i:
                sql = "INSERT INTO md_stream (symbol, datetime, open, high, low, close, volume, notes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                val = (
                        i,  #symbol
                        current_tick_dict.get('e'), #datetime
                        current_tick_dict['o'],  #open
                        current_tick_dict['h'],  #high
                        current_tick_dict['l'],  #low
                        current_tick_dict['c'],  #close
                        current_tick_dict['v'],   #volume
                        None  #notes
                        ) 

                # print ("i, val :::", i, val, "sql::::", sql)

                cursor.execute(sql, val)  #this part has issue
                connection.commit()
                print (val, ' - {} committed'.format(current_tick_dict['sym']))

def on_error(ws, error):
	print(error)

def on_close(ws):
    print ("closed connection, try to reconnect")

ws = websocket.WebSocketApp(config.POLYGON_MD_SOCKET,on_open = on_open, on_message=on_message, on_close=on_close, on_error=on_error)

while True:
    try:
       ws.run_forever()
    except:
        print ('cannot reconnect')
