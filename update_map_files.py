from AlgorithmImports import *
import os
import pandas as pd
import zipfile
import sys
from datetime import datetime
from typing import List

def get_tickers_market(ticker: str):
    return {
        'RTY': 'CME',
        'NQ': 'CME',
        'ES': 'CME',
        '6A': 'CME',
        '6B': 'CME',
        '6C': 'CME',
        '6E': 'CME',
        '6J': 'CME',
        '6S': 'CME',
        '6N': 'CME',
        'CL': 'NYMEX',
        'NG': 'NYMEX',
        'GC': 'COMEX',
        'HG': 'COMEX',
        'SI': 'COMEX',
        'ZS': 'CBOT',
        'ZN': 'CBOT',
        'BTCUSDT.P': 'BINANCE',
        'BTCUSDT': 'BINANCE'
    }[ticker.upper()]

def get_zipped_files_names(zip_file_name: str):
    with zipfile.ZipFile(zip_file_name, 'r') as zip_ref:
        file_names = zip_ref.namelist()
    return file_names


_algo: QCAlgorithm = None
def print(*args, **kwargs):
    _algo.Log(args)
    # _algo.Log(kwargs)

def update_map_files(algo: QCAlgorithm, subset: List[str]= None):
    global _algo
    _algo = algo

    data_base_path = Globals.DataFolder
    future_base_path = os.path.join(data_base_path, 'future')

    for market in os.listdir(rf'{future_base_path}'):
        market_dir_path = os.path.join(future_base_path, market)

        if not os.path.isdir(market_dir_path):
            continue
        map_files_dir_path = os.path.join(market_dir_path, 'map_files')
        if not os.path.exists(map_files_dir_path):
            os.makedirs(map_files_dir_path)

        if not os.path.exists(os.path.join(market_dir_path, 'minute')):
            continue

        minute_path = os.path.join(market_dir_path,'minute')


        for ticker in os.listdir(minute_path):
            if subset is not None and ticker not in subset and ticker.upper() not in subset:
                continue
            print(f'WORKING ON {ticker}')
            ticker_dir_path = os.path.join(minute_path, ticker)
            df = pd.DataFrame(columns=['Date', 'SecurityId', 'Market', 'DataMappingMode'])

            for zip_filename in os.listdir(ticker_dir_path):
                date = zip_filename[:zip_filename.find('_')]
                literal_date = date
                date = datetime.strptime(date, '%Y%m%d')

                zip_full_path = os.path.join(ticker_dir_path, zip_filename)
                zipped_file_name = get_zipped_files_names(zip_full_path)[0]

                rollover_date = zipped_file_name[zipped_file_name.rfind('_')+1:zipped_file_name.rfind('.')]
                rollover_date = datetime.strptime(rollover_date, '%Y%m%d')

                contract_id = SecurityIdentifier.GenerateFuture(rollover_date, ticker, market).ToString().lower()


                for dataMappingMode in range(4):
                    cur_row = pd.DataFrame({'Date': literal_date,
                                            'SecurityId': contract_id.lower(),
                                            'Market': market.upper(),
                                            'DataMappingMode': dataMappingMode},
                                           index=[df.shape[0]])

                    df = pd.concat([df, cur_row])

            map_file_dest_path = os.path.join(map_files_dir_path, f'{ticker}.csv')
            df = df.sort_values(by=['Date', 'DataMappingMode'])
            df.to_csv(map_file_dest_path, index=False, columns=['Date', 'SecurityId', 'Market', 'DataMappingMode'])