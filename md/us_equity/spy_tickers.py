import urllib.request
import pandas as pd

def spy_list_download():
    spy_holdings="https://www.ssga.com/us/en/institutional/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx"
    spy_list = urllib.request.urlretrieve(spy_holdings, "spy_list.xlsx")
    print ("SPY List downloaded")

# spy_list_download()

def spy_tickers():
    data = pd.read_excel("spy_list.xlsx", 
                        sheet_name="holdings", skiprows=4,
                        usecols=["Name","Ticker","Identifier","SEDOL","Weight","Sector","Shares Held","Local Currency"])

    data = data.dropna(axis=0,subset=['Local Currency'])

    spy_tickers = data['Ticker'].to_list()
    spy_tickers.remove("CASH_USD")
    return spy_tickers

# print (len(spy_tickers()))