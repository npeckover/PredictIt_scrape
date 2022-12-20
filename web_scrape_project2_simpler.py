import psycopg2
import configparser
import requests
import pytz

# establish connection
tz = pytz.timezone('US/Pacific')
parser = configparser.ConfigParser()
parser.read('database.ini')
params = parser.items('postgresql')
db = {}
for param in params:
    db[param[0]] = param[1]
conn = psycopg2.connect(**db)
cur = conn.cursor()

# get market data
response = requests.get('https://www.predictit.org/api/marketdata/all/')
data = response.json()['markets']

def create_tables():
    """ create tables in the PredictIt database"""
    commands = (
        """ 
        CREATE TABLE IF NOT EXISTS tickers (
                ticker_id INTEGER PRIMARY KEY,
                ticker_name VARCHAR(255),
                ticker_short_name VARCHAR(255),
                ticker_timestamp VARCHAR(255),
                ticker_status VARCHAR(255),
                ticker_image VARCHAR(255),
                ticker_url VARCHAR(255)
        )
        """,
        """
        CREATE TABLE contract_offers2 (
                ticker_id INTEGER,
                contract_id INTEGER,
                contract_name VARCHAR(255),
                contract_short_name VARCHAR(255),
                contract_last_trade_price REAL,
                contract_best_buy_yes REAL, 
                contract_best_buy_no REAL, 
                contract_best_sell_yes REAL, 
                contract_best_sell_no REAL, 
                contract_last_close_price REAL, 
                contract_status VARCHAR(255)
        );
        DROP TABLE contract_offers;
        ALTER TABLE contract_offers2 RENAME TO contract_offers
        """)
    # create tables one by one
    for command in commands:
        cur.execute(command)
if __name__ == '__main__':
    create_tables()

# extract data from json and upload to sql db
for i in range(len(data)):
    ticker_id = data[i]['id']
    ticker_name = data[i]['name']
    ticker_short_name = data[i]['shortName']
    ticker_timestamp = data[i]['timeStamp']
    ticker_status = data[i]['status']
    ticker_image = data[i]['image']
    ticker_url = data[i]['url']
    cur.execute(
        """
        INSERT INTO tickers (
        ticker_id, ticker_name, 
        ticker_short_name, ticker_timestamp, 
        ticker_status, ticker_image, ticker_url
        ) 
        values (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(ticker_id) DO UPDATE 
        SET ticker_id = excluded.ticker_id
        """, 
        (ticker_id, ticker_name, 
        ticker_short_name, ticker_timestamp, 
        ticker_status, ticker_image, ticker_url,)
        )
    for j in range(len(data[i]['contracts'])):
        contract_id = data[i]['contracts'][j]['id']
        contract_name = data[i]['contracts'][j]['name']
        contract_short_name = data[i]['contracts'][j]['shortName']
        contract_last_trade_price = data[i]['contracts'][j]['lastTradePrice']
        contract_best_buy_yes = data[i]['contracts'][j]['bestBuyYesCost']
        contract_best_buy_no = data[i]['contracts'][j]['bestBuyNoCost']
        contract_best_sell_yes = data[i]['contracts'][j]['bestSellYesCost']
        contract_best_sell_no = data[i]['contracts'][j]['bestSellNoCost']
        contract_last_close_price = data[i]['contracts'][j]['lastClosePrice']
        contract_status = data[i]['contracts'][j]['status']
        cur.execute(
            """
            INSERT INTO contract_offers (
            ticker_id, contract_id,
            contract_name, contract_short_name,
            contract_last_trade_price,
            contract_best_buy_yes, contract_best_buy_no, 
            contract_best_sell_yes, contract_best_sell_no, 
            contract_last_close_price, contract_status
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (ticker_id, contract_id,
                contract_name, contract_short_name,
                contract_last_trade_price,
                contract_best_buy_yes, contract_best_buy_no, 
                contract_best_sell_yes, contract_best_sell_no, 
                contract_last_close_price, contract_status,)
        )

# commit changes and close connection   
cur.close()
conn.commit()
conn.close()
print("Complete")