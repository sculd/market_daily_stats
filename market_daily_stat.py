import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')
import pytz
from google.cloud import bigquery
import record
from enum import Enum
import time_util
import logging_util

_TIMEZONE_US_EAST = pytz.timezone('US/EASTERN')
_DATASET_ID_STOCK = 'market_data'
_DATASET_ID_BINANCE = 'market_data_binance'

_QUERY_FORMAT = """
    SELECT 
        symbol, cur_date, 
        ROUND(daily_max, 2) AS daily_max, 
        ROUND(daily_min, 2) AS daily_min, 
        ROUND(daily_avg, 2) AS daily_avg, 
        ROUND(daily_volume / 1000000, 3) AS daily_volume_millions	
    FROM `trading-290017.{dataset_id}.daily_aggregation`
    WHERE TRUE
    AND cur_date = "{date_str}"
    """

_RESOURCE_DYNAMODB = 'dynamodb'
_TABLE_NAME = 'market_daily_stat'
_EVENT_KEY_QUERY_STRING_PARAMETER = 'queryStringParameters'
_PARAM_KEY_DATE_STR = 'date_str'
_PARAM_KEY_MARKET = 'market'
_PARAM_KEY_SYMBOL = 'symbol'
_DATABASE_KEY_SYMBOL = 'symbol'
_DATABASE_KEY_DATE_STR = 'date_str'
_DATABASE_KEY_MARKET = 'market'
_RESPONSE_KEY_DATE = 'date'
_RESPONSE_KEY_DATETIME = 'datetime'
_client = bigquery.Client()


class DatasetMode(Enum):
    STOCK = 1
    BINANCE = 2

def _get_daily_stat(date_str, dataset_mode):
    dataset_id = DatasetMode.STOCK
    if dataset_mode is DatasetMode.STOCK:
        dataset_id = _DATASET_ID_STOCK
    elif dataset_mode is DatasetMode.BINANCE:
        dataset_id = _DATASET_ID_BINANCE

    market = 'stock'
    if dataset_mode is DatasetMode.STOCK:
        market = 'stock'
    elif dataset_mode is DatasetMode.BINANCE:
        market = 'binance'

    query = _QUERY_FORMAT.format(date_str=date_str, dataset_id=dataset_id)
    q_job = _client.query(query)
    results = q_job.result()
    ret = []
    for row in results:
        item = {
            'symbol': row.symbol,
            'market': market,
            'cur_date': row.cur_date.strftime('%Y-%m-%d'),
            'daily_max': row.daily_max,
            'daily_min': row.daily_min,
            'daily_avg': row.daily_avg,
            'daily_volume_millions': row.daily_volume_millions
        }
        ret.append(item)
    return ret

def update_today_daily_stat(date_str = None):
    date_str = date_str or time_util.get_today_date_str()
    daily_stat_stock = _get_daily_stat(date_str, DatasetMode.STOCK)
    logging_util.info('batch recording {l} items for stock'.format(l=len(daily_stat_stock)))
    record.records(daily_stat_stock)
    daily_stat_binance = _get_daily_stat(date_str, DatasetMode.BINANCE)
    logging_util.info('batch recording {l} items for binance'.format(l=len(daily_stat_binance)))
    record.records(daily_stat_binance)
