'''
Auther : Milad Ranaei
'''
import requests
import json
import pandas as pd   
import logging
import asyncio
import time

list_pair = ['USDC_XRP', 'USDC_LTC', 'USDT_BCH', 'USDT_ETH']
async def kraken_output(sleep):
    '''
    parameter : Get a time in second as a parameter
    '''
    await asyncio.sleep(sleep)
    ex_name = "kraken"
    try:
        response = requests.request("GET", url= "https://futures.kraken.com/derivatives/api/v3/tickers")
    except requests.ConnectionError as error:
        logging.warning(error)
    post_data = json.loads(response.content)
    post_data = post_data['tickers']
    df = pd.DataFrame(post_data, columns=['pair', 'ask', 'bid'])
    df = df.groupby(['pair']).sum().sort_values(by='bid', ascending=True)
    df['exchange'] = ex_name
    print(df)

async def poloniex_output(sleep):
    '''
    parameter : Get a time in second as a parameter
    '''
    await asyncio.sleep(sleep)
    ex_name = "poloniex"
    new_keys = []
    data_poloniex = []
    response = requests.request("GET", url= "https://poloniex.com/public?command=returnTicker")
    post_data = json.loads(response.content)
    keys = post_data.keys()

    for key in keys:
        if key in list_pair:
            pair_data = post_data[key]
            pair_data['pair'] = key
            data_poloniex.append(pair_data)

    df2 = pd.DataFrame(data_poloniex, columns=['pair', 'lowestAsk', 'highestBid'])
    df2.rename(columns = {'lowestAsk': 'ask', 'highestBid':'bid'}, inplace = True)
    df2 = df2.sort_values(by='bid', ascending=True)
    df2['exchange'] = ex_name
    df2.set_index('pair', inplace=True)
    df2.columns = [''] * len(df2.columns)
    df2.rename_axis(None, inplace=True)
    print(df2)




async def main():
    logging.warning(f"Started: {time.strftime('%X')}")
    while True:
        await kraken_output(10)
        await poloniex_output(10)

# Python 3.7+
asyncio.run(main())