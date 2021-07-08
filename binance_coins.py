import pandas as pd
import numpy as np
from binance.client import Client
from binance.exceptions import BinanceAPIException
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import itertools as it
import os
import json
import math
import time, argparse
from datetime import datetime, timezone, timedelta

binance_api_key = ""
binance_api_secret_key = ""
first_n_coins = 150
top_n_ranked_coins = 60
correlation_greater_than = 0.70
correlation_less_than = 1
paired_coin = "BTC"
history_end = datetime.now().astimezone(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
#history_end = (datetime.now().astimezone(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)) - timedelta(days = 1)
#history_end = datetime.now().replace(tzinfo=timezone.utc).astimezone(tz=None).replace(hour=0, minute=0, second=0, microsecond=0)
history_delta = 7
history_start = None
history_interval = Client.KLINE_INTERVAL_1MINUTE
coin_history_file = 'historical_klines.json'
used_coins_file = 'used_coins'
ignored_coins_file = 'ignored_coins'
client = Client()

def get_coins_from_file(file):
    supported_coin_list = []

    if os.path.exists(file):
        with open(file) as rfh:
            for line in rfh:
                line = line.strip()
                if not line or line.startswith("#") or line in supported_coin_list:
                    continue
                supported_coin_list.append(line)
    else:
        raise Exception("Coin list not found")

    return supported_coin_list


def get_all_tickers(bridge):
    coins = []
    
    for ticker in client.get_all_tickers():
        if bridge in ticker['symbol'] and ticker['symbol'].replace(bridge, "") in get_coins_from_file(used_coins_file):
            coins.append(ticker['symbol'].replace(bridge, ''))
    return coins


def klines_to_df(klines):
    df = pd.DataFrame.from_records(klines, columns=['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                                   'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
    df['change'] = df.apply(lambda row: (
        (float(row.close) - float(row.open))/float(row.open))*100, axis=1)
    df['normalized'] = (df['close'].astype('float') - df['close'].astype('float').min()) / \
        (df['close'].astype('float').max()-df['close'].astype('float').min())
    return df


def pearson_correlation(x, y):
    lenght = len(x) if len(x) <= len(y) else len(y)
    meanx = sum(x)/lenght
    meany = sum(y)/lenght

    num = 0
    for i in range(lenght):
        num += ((x[i]-meanx)*(y[i]-meany))

    denx = 0
    deny = 0
    for i in range(lenght):
        denx += pow(x[i]-meanx, 2)
        deny += pow(y[i]-meany, 2)

    den = math.sqrt(denx*deny)

    return num/den


def get_all_coins_combinations(coin_list):
    filtered_coin_list = []
    combinations = []

    for coin in coin_list:
        filtered_coin_list.append(coin)

    for combination in list(it.product(filtered_coin_list, repeat=2)):
        if(combination[0] != combination[1]):
            combinations.append(combination)

    output = set(map(lambda x: tuple(sorted(x)), combinations))

    return output


def get_one_coin_combinations(coin_list, coin):
    combinations = []
    for c in coin_list:
        if(c != coin):
            combinations.append((c, coin))

    return combinations


def get_coins_history(coin_list, bridge):
    klines = {}

    end = str(history_end.timestamp())
    start = str(history_start.timestamp())

    print('Fetching trade data between "' + history_start.replace(tzinfo=timezone.utc).astimezone(tz=None).strftime('%d %B %Y %H:%M:%S') + '" and "' + history_end.replace(tzinfo=timezone.utc).astimezone(tz=None).strftime('%d %B %Y %H:%M:%S') + '"')

    count = 0
    for coin in coin_list:

        print("Getting "+coin+bridge+" history data... " +
              str(round((count*100)/len(coin_list))) + "%")
        try:
            coin_klines = client.get_historical_klines(
                coin+bridge, history_interval, start, end)
            klines[coin] = coin_klines
        except BinanceAPIException as e:
            print("Error"+str(e))
            pass
        count = count + 1

    return klines


def get_existing_coins(coin_list, coins_history):
    existing_coins = []
    for coin in coin_list:
        if coin in coins_history:
            existing_coins.append(coin)
    return existing_coins


def get_one_correlated_values(correlated_coin):
    verify_coins_files()

    coins_history = read_coins_history_file()
    ignored_coins = get_coins_from_file(
        ignored_coins_file) if os.path.isfile(ignored_coins_file) else []

    coin_list = []
    [coin_list.append(x) for x in get_coins_from_file(
        used_coins_file)[:first_n_coins] if x in coins_history and x not in ignored_coins]

    if correlated_coin not in coins_history:
        raise Exception("Coin not found")

    correlations = []
    sorted_correlations = {}

    combinations = get_one_coin_combinations(
        coin_list, correlated_coin)

    for coins in combinations:
        correlations.append({"coin_a": coins[0], "coin_b": coins[1], "correlation": pearson_correlation(
            coins_history[coins[0]]['normalized'].tolist(), coins_history[coins[1]]['normalized'].tolist())})

    filtered_correlations = [
        c for c in correlations if c['correlation'] > correlation_greater_than and c['correlation'] <= correlation_less_than]
    sorted_correlations = sorted(
        filtered_correlations, key=lambda i: i['correlation'])

    for c in sorted_correlations:
        print(c['coin_a']+"/"+c['coin_b']+": "+str(round(c['correlation'], 2)))


def get_one_correlated_list(correlated_coin):
    verify_coins_files()

    coins_history = read_coins_history_file()
    ignored_coins = get_coins_from_file(
        ignored_coins_file) if os.path.isfile(ignored_coins_file) else []

    coin_list = []
    [coin_list.append(x) for x in get_coins_from_file(
        used_coins_file)[:first_n_coins] if x in coins_history and x not in ignored_coins]

    if correlated_coin not in coins_history:
        raise Exception("Coin not found")

    correlations = []
    sorted_correlations = {}

    combinations = get_one_coin_combinations(
        coin_list, correlated_coin)

    for coins in combinations:
        correlations.append({"coin_a": coins[0], "coin_b": coins[1], "correlation": pearson_correlation(
            coins_history[coins[0]]['normalized'].tolist(), coins_history[coins[1]]['normalized'].tolist())})

    filtered_correlations = [
        c for c in correlations if c['correlation'] > correlation_greater_than and c['correlation'] <= correlation_less_than]
    sorted_correlations = sorted(
        filtered_correlations, key=lambda i: i['correlation'])

    correlated_coin_list = []
    filtered_correlated_coin_list = []

    for c in sorted_correlations:
        correlated_coin_list.append(c['coin_a'])
        correlated_coin_list.append(c['coin_b'])

    [filtered_correlated_coin_list.append(
        x) for x in correlated_coin_list if x not in filtered_correlated_coin_list]

    print(sorted(filtered_correlated_coin_list))


def get_all_correlated_values():
    verify_coins_files()

    coins_history = read_coins_history_file()
    ignored_coins = get_coins_from_file(
        ignored_coins_file) if os.path.isfile(ignored_coins_file) else []

    coin_list = []
    [coin_list.append(x) for x in get_coins_from_file(
        used_coins_file)[:first_n_coins] if x in coins_history and x not in ignored_coins]

    correlations = []
    sorted_correlations = {}

    combinations = get_all_coins_combinations(coin_list)

    for coins in combinations:
        correlations.append({"coin_a": coins[0], "coin_b": coins[1], "correlation": pearson_correlation(
            coins_history[coins[0]]['normalized'].tolist(), coins_history[coins[1]]['normalized'].tolist())})

    filtered_correlations = [
        c for c in correlations if c['correlation'] > correlation_greater_than and c['correlation'] <= correlation_less_than]
    sorted_correlations = sorted(
        filtered_correlations, key=lambda i: i['correlation'])

    for c in sorted_correlations:
        print(c['coin_a']+"/"+c['coin_b']+": "+str(round(c['correlation'], 2)))


def get_all_correlated_grouped():
    verify_coins_files()

    coins_history = read_coins_history_file()
    ignored_coins = get_coins_from_file(
        ignored_coins_file) if os.path.isfile(ignored_coins_file) else []
    coin_list = []
    [coin_list.append(x) for x in get_coins_from_file(
        used_coins_file)[:first_n_coins] if x in coins_history and x not in ignored_coins]

    correlations = []

    combinations = get_all_coins_combinations(coin_list)

    for coins in combinations:
        correlations.append({"coin_a": coins[0], "coin_b": coins[1], "correlation": pearson_correlation(
            coins_history[coins[0]]['normalized'].tolist(), coins_history[coins[1]]['normalized'].tolist())})

    filtered_correlations = [
        c for c in correlations if c['correlation'] > correlation_greater_than and c['correlation'] <= correlation_less_than]

    group_correlations(filtered_correlations)


def get_all_correlated_list():
    verify_coins_files()

    coins_history = read_coins_history_file()
    ignored_coins = get_coins_from_file(
        ignored_coins_file) if os.path.isfile(ignored_coins_file) else []
    coin_list = []
    [coin_list.append(x) for x in get_coins_from_file(
        used_coins_file)[:first_n_coins] if x in coins_history and x not in ignored_coins]

    correlations = []

    combinations = get_all_coins_combinations(coin_list)

    for coins in combinations:
        correlations.append({"coin_a": coins[0], "coin_b": coins[1], "correlation": pearson_correlation(
            coins_history[coins[0]]['normalized'].tolist(), coins_history[coins[1]]['normalized'].tolist())})

    filtered_correlations = [
        c for c in correlations if c['correlation'] > correlation_greater_than and c['correlation'] <= correlation_less_than]

    correlated_coin_list = []
    filtered_correlated_coin_list = []

    for c in filtered_correlations:
        correlated_coin_list.append(c['coin_a'])
        correlated_coin_list.append(c['coin_b'])

    [filtered_correlated_coin_list.append(
        x) for x in correlated_coin_list if x not in filtered_correlated_coin_list]

    print(sorted(filtered_correlated_coin_list))


def group_correlations(correlations):
    l = [(c["coin_a"], c["coin_b"])
         for c in correlations]
    pool = set(map(frozenset, l))
    groups = []
    coin_groups = []
    while pool:
        group = set()
        groups.append([])
        while True:
            for candidate in pool:
                if not group or group & candidate:
                    group |= candidate
                    groups[-1].append(tuple(candidate))
                    pool.remove(candidate)
                    break
            else:
                break

    for g in groups:
        separated = []
        coin_list = []
        for c in g:
            separated.append(c[0])
            separated.append(c[1])
        for x in separated:
            if(x not in coin_list):
                coin_list.append(x)
        coin_groups.append(coin_list)

    for i in range(len(coin_groups)):
        print("Group "+str(i+1)+":")
        print(sorted(coin_groups[i]))


def verify_coins_files():
    if not os.path.isfile(coin_history_file):
        raise Exception(
            "Coin history '"+coin_history_file+"' not found, please run: binance_api.py --update-coins-history")

    if not os.path.isfile(used_coins_file):
        raise Exception(
            "Top coins file '"+used_coins_file+"' not found, please run: binance_api.py --update-top-coins")


def update_coin_historical_klines():
    coins_history = get_coins_history(
        get_all_tickers(paired_coin), paired_coin)
    with open(coin_history_file, 'w') as outfile:
        json.dump(coins_history, outfile)


def read_coins_history_file():
    kline_df = {}
    data = {}

    with open(coin_history_file) as json_file:
        data = json.load(json_file)

    for coin in data:
        if(len(data[coin]) > 0):
            kline_df[coin] = klines_to_df(data[coin])

    return kline_df


def update_top_ranked_coins():
    headers = {
        'Accepts': 'application/json',
    }

    ignored_coins = []

    # get stablecoin list
    url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=stablecoins&order=market_cap_desc&per_page=250&page=1&sparkline=false'
    session = Session()
    session.headers.update(headers)
    response = session.get(url)
    raw_list = json.loads(response.text)
    for coin in raw_list:
      ignored_coins.append(coin['symbol'].upper())

    # get compond token list
    url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=compound-tokens&order=market_cap_desc&per_page=250&page=1&sparkline=false'
    session = Session()
    session.headers.update(headers)
    response = session.get(url)
    raw_list = json.loads(response.text)
    for coin in raw_list:
      ignored_coins.append(coin['symbol'].upper())

    # get compond token list
    url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=aave-tokens&order=market_cap_desc&per_page=250&page=1&sparkline=false'
    session = Session()
    session.headers.update(headers)
    response = session.get(url)
    raw_list = json.loads(response.text)
    for coin in raw_list:
      ignored_coins.append(coin['symbol'].upper())

    # get wrapped token list
    url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=wrapped-tokens&order=market_cap_desc&per_page=250&page=1&sparkline=false'
    session = Session()
    session.headers.update(headers)
    response = session.get(url)
    raw_list = json.loads(response.text)
    for coin in raw_list:
      ignored_coins.append(coin['symbol'].upper())

    # get eth 2.0 staking token list
    url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=eth-2-0-staking&order=market_cap_desc&per_page=250&page=1&sparkline=false'
    session = Session()
    session.headers.update(headers)
    response = session.get(url)
    raw_list = json.loads(response.text)
    for coin in raw_list:
      ignored_coins.append(coin['symbol'].upper())

    # get top 250 coins
    url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=1&sparkline=false'
    session = Session()
    session.headers.update(headers)
    response = session.get(url)
    data = json.loads(response.text)

    fullList = {}

    targetDate = history_end.strftime('%d-%m-%Y')
    
    print("Fetching trade volume data for " + history_end.replace(tzinfo=timezone.utc).astimezone(tz=None).strftime('%d %B %Y'))
    for coin in data:
        if any([x in coin['symbol'].upper() for x in ['BULL', 'BEAR','UP', 'DOWN', 'HEDGE', 'LONG', 'SHORT']]) or coin['symbol'].upper() in ignored_coins:
            data.remove(coin)
            continue

        url = 'https://api.coingecko.com/api/v3/coins/' + str(coin['id']) + "/history?date=" + str(targetDate) + "&localization=false"

        session = Session()
        session.headers.update(headers)

        response = session.get(url)
        history = json.loads(response.text)

        try:
            print(str(history['symbol']).upper() + ' ## ' + str(history['market_data']['total_volume']['usd']))
            fullList[history['symbol'].upper()] = int(history['market_data']['total_volume']['usd'])   
        except:
            print(str(history['symbol']).upper() + ' ## unavailable!' )
            pass

        time.sleep(1.3)
        
    print("Parsing top "+str(top_n_ranked_coins)+" coins...")
    try:
        with open(used_coins_file, 'w') as writer:
            # Sort shortList by value
            for coin in sorted(fullList, key=fullList.get, reverse=True)[:top_n_ranked_coins]:
                if float(fullList[coin]) > 0:
                    writer.write(coin+'\n')

        print("Top coin list stored successfully!")
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)


def load_configuration(args):
    global first_n_coins, top_n_ranked_coins, correlation_greater_than, correlation_less_than, paired_coin, history_start, history_delta, history_end, history_interval, coin_history_file, used_coins_file, ignored_coins_file

    # read optional args
    if args["start_datetime"]:
      try:
        #history_start = datetime.strptime(args["start_datetime"][0], '%Y-%m-%d.%H:%M:%S').replace(tzinfo=timezone.utc).astimezone(tz=None)
        history_start = datetime.strptime(args["start_datetime"][0], '%Y-%m-%d.%H:%M:%S').astimezone(tz=timezone.utc)
      except:
        print('Invalid Date format - expected : "%Y-%m-%d.%H:%M:%S"')
        exit()

    if args["end_datetime"]:
      try:
        #history_end = datetime.strptime(args["end_datetime"][0], '%Y-%m-%d.%H:%M:%S').replace(tzinfo=timezone.utc).astimezone(tz=None)
        history_end = datetime.strptime(args["end_datetime"][0], '%Y-%m-%d.%H:%M:%S').astimezone(tz=timezone.utc)
      except:
        print('Invalid Date format - expected : "%Y-%m-%d.%H:%M:%S"')
        exit()

    if args["date_offset"] and int(args["date_offset"][0]) > 0:
      try:
        history_delta = int(args["date_offset"][0])
      except:
        print('Offset must be positive - expected : INT > 0')
        exit()

    if args["paired_coin"]:
      try:
        paired_coin = str(args["paired_coin"][0])
      except:
        pass

    if history_start is None:
      history_start = (history_end - timedelta(days = history_delta))


def main(args):

  load_configuration(args)

  if args["update_top_coins"]:
    update_top_ranked_coins()
  
  if args["update_coins_history"]:
    update_coin_historical_klines()

  if args["all_correlated_values"]:
      get_all_correlated_values()
  
  if args["one_correlated_values"]:
      get_one_correlated_values(args["one_correlated_values"][0])
  
  if args["all_correlated_list"]:
      get_all_correlated_list()
  
  if args["one_correlated_list"]:
      get_one_correlated_list(args["one_correlated_list"][0])
  
  if args["all_correlated_grouped"]:
      get_all_correlated_grouped()