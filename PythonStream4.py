# -*- coding: utf-8 -*-
"""
Created on Tue Jul 20 15:33:11 2021

@author: Ashwini
"""


import json, requests, time
import numpy as np
import datetime as dt
from datetime import datetime
from datetime import timedelta
import sys
import pandas as pd
 
  
params = {
  # crypto setup
  'currency_1': 'BTC', # Bitcoin
  'currency_2': 'ETH', # Ethereum
  'currency_3': 'ADA', # Chainlink
  'ref_currency': 'USD',
}

# real time data collector
def getCryptoRealTimeData(crypto,start_date,end_date,gran):
        t_0 = time.time()
        # call API
        uri = 'https://api.pro.coinbase.com/products/{0}-{1}/{2}?start={3}&end={4}&granularity={5}'.format(crypto, params['ref_currency'], 'candles',start_date,end_date,gran)
        #print(uri)
        res = requests.get(uri)

        if (res.status_code==200):
            # read json response
            raw_data = json.loads(res.content)
       
            for i in range(len(raw_data)):
                new_data = {
                "cryptocurrency":crypto,
                "timestamp":raw_data[i][0],
                "low":raw_data[i][1],
                "high":raw_data[i][2],
                "open":raw_data[i][3],
                "close":raw_data[i][4],
                "volume":raw_data[i][5]
                }
              
                f.write(json.dumps(new_data))
                f.write('\n')
                print(json.dumps(new_data))
            # debug / print message
            print('API request at time {0}'.format(dt.datetime.utcnow()))
            
            # produce record to kafka
            #produceRecord(new_data, producer, topic)
            # debug \ message in prompt
            # print('Produce record to topic \'{0}\' at time {1}'.format(topic, dt.datetime.utcnow()))
            
        else:
            print('Failed API request at time {0}'.format(dt.datetime.utcnow()))

if __name__ == "__main__":
    start_date = "2017-01-01"
    start_date =  datetime.strptime(start_date, "%Y-%m-%d")
    f = open("cryptodata.txt", "a")
    while(start_date.date() <= datetime.today().date()):
        getCryptoRealTimeData(params['currency_1'], start_date.date(),(start_date + timedelta(days=300)).date(),86400),
        getCryptoRealTimeData(params['currency_2'], start_date.date(),(start_date + timedelta(days=300)).date(),86400),
        getCryptoRealTimeData(params['currency_3'], start_date.date(),(start_date + timedelta(days=300)).date(),86400),
        start_date = (start_date+timedelta(days=300)) 
    f.close()
