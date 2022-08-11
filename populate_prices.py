from distutils.command.config import config
from multiprocessing import connection
from re import S
from numpy import busday_count
from config import *
import alpaca_trade_api as tradeapi
import sqlite3
from alpaca_trade_api.rest import REST, TimeFrame

connection = sqlite3.connect(DB_FILE)

connection.row_factory = sqlite3.Row

cursor = connection.cursor()

cursor.execute("""
    SELECT id, symbol, name FROM stock
""")

rows = cursor.fetchall()

symbols = [] 
stock_dict = {}
for row in rows:
    symbol = row['symbol']
    symbols.append(symbol)
    stock_dict[symbol] = row['id']
    
    
#symbols = ['RYTM']   
api = tradeapi.REST(API_KEY, API_SECRET, DATA_URL)
#print(stock_dict)
chunk_size = 200
for i in range(0, len(symbols), chunk_size):
    symbol_chunk = symbols[i:i+chunk_size]
     
    barsets = api.get_bars(symbol_chunk, TimeFrame.Day, "2022-01-01", "2022-08-10", adjustment='raw')
    for symbol in barsets:
        print(f"processing stock {symbol.S}")
        if stock_dict[symbol.S]: 
                stock_id = stock_dict[symbol.S]
                #print(stock_dict[symbol.S])        
                cursor.execute("""
                    INSERT INTO stock_price (stock_id, date, open, high, low, close, volume)
                    VALUES (?,?,?,?,?,?,?)
                """, (stock_id, symbol.t.date(), symbol.o, symbol.h, symbol.l, symbol.c, symbol.v))

connection.commit()
