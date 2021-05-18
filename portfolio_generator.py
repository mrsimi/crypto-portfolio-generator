from binance.client import Client
import os

from pandas.core import base 
import social_sentiment as social_snt
import pandas as pd
from requests import Request, Session
import requests
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
from itertools import islice
import time

class PorfolioGenerator:
    def __init__(self) :
        api_key = os.environ.get('api_key')
        api_secret = os.environ.get('secret_key')

        self.client = Client(api_key, api_secret)  
        self.percent_change_lower = 5.0 # the percentage change in asset lower limit
        self.percent_change_higher = 8.0  # the percentage change in asset higher limit 
    
    def chunks(self, data, SIZE=10000):
        it = iter(data)
        for i in range(0, len(data), SIZE):
            yield {k:data[k] for k in islice(it, SIZE)}    
    
    def get_by_volume(self):
        tickers = self.client.get_ticker()
        df_tickers = pd.DataFrame.from_dict(tickers)
        
        mean_volume = df_tickers['volume'].astype(float).median()

        df_filtered_by_volume = df_tickers[(df_tickers.volume.astype(float) >= mean_volume)]
        df_filtered_by_volume = df_filtered_by_volume[(df_filtered_by_volume.priceChangePercent.astype(float) >= self.percent_change_lower) & (df_filtered_by_volume.priceChangePercent.astype(float) <= self.percent_change_higher)] 
        
        df_50 = df_filtered_by_volume[:50]
        
        symbols = df_50['symbol']
        
        return symbols
        
    def get_baseAsset(self, symbols):
        exchange_query = ""
        exchange_query = '","'.join(symbols)

        exchange_query = '["'+exchange_query+'"]'
        
        symbol_url = 'https://api.binance.com/api/v3/exchangeInfo?symbols='
        
        #print(exchange_query)
        res_url = symbol_url+exchange_query
        response = requests.get(res_url)
        symbol_data = json.loads(response.text)

        data_symbols = symbol_data['symbols']

        pd_values = pd.json_normalize(data_symbols)
        
        base_assets = pd_values['baseAsset']
        
        pd_json = pd_values[['symbol', 'status', 'baseAsset']]
        pd_json.to_csv('assets.csv', index=False)
        
        return base_assets
    
    def get_cmc_data(self, base_assets):
        base_assets = base_assets.tolist()
        #base_assets = base_assets.remove('BQX')
        query = ""
        query = ",".join(base_assets)

        
        
        url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/info'
        parameters = {
        'symbol':query,
        'aux': 'urls'
        }
        headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': os.environ.get('coin_mkcap_api'),
        }

        session = Session()
        session.headers.update(headers)

        try:
            response = session.get(url, params=parameters)
            cmc_data = json.loads(response.text)
            #print(cmc_data)
        except (ConnectionError, Timeout, TooManyRedirects) as e:
            print(e['status']['error_message'])
            
        df_cmc_data = pd.DataFrame.from_dict(cmc_data['data'])
        
        return df_cmc_data
    
    def get_twitter_report(self, df_cmc_data):
        cmc_dict = {}
        cmc_names = {}

        for (col_name, col_data) in df_cmc_data.iteritems():
            cmc_dict[col_name] = df_cmc_data[col_name].urls
            cmc_names[col_name] = df_cmc_data[col_name].slug
            
        #sentiment analysis 
        twt_client = social_snt.TwitterClient()

        tw_sent_report = []

        for item in self.chunks(cmc_names, 5):
            #print(item)

            for index ,(key, value) in enumerate(item.items()):
                #print(value)
                
                tweets = twt_client.get_tweets(query=value, count=200)
                #print(tweets)

                if tweets is not None and len(tweets) > 0:
                    ptweets = [tweet for tweet in tweets if tweet['sentiment'] == 'positive']
                    ntweets = [tweet for tweet in tweets if tweet['sentiment'] == 'negative']
                    
                    npercent = 0
                    ppercent = 0
                    if len(ntweets) > 0:
                        npercent = len(ntweets) / len(tweets)
                    if len(ptweets) > 0:
                        ppercent = len(ptweets)/ len(tweets)

                    ntpercent = 1 - (npercent + ppercent)

                    tw_sent_report.append(social_snt.SentimentReport(key, ppercent, npercent, ntpercent))
                else: 
                    pass
            
            time.sleep(20)
        
        json_report = json.dumps([ob.__dict__ for ob in tw_sent_report], indent=4)
        pd_obj = pd.read_json(json_report, orient='Index')
        csv_data = pd_obj.to_csv('porfolio.csv', index=False)
        
    def get_portfolio_volume_nd_sentiment(self):
        volume = self.get_by_volume()
        base_assets = self.get_baseAsset(volume)
        cmc_data = self.get_cmc_data(base_assets)
        self.get_twitter_report(cmc_data)
            
        

