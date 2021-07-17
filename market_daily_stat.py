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
    WITH BASE AS (
        SELECT *, DATE(timestamp, "America/New_York") AS cur_date
        FROM `trading-290017.{dataset_id}.by_minute`
        WHERE TRUE
        AND (EXTRACT(HOUR FROM timestamp AT TIME ZONE "America/New_York") < 16)
        AND ((EXTRACT(HOUR FROM timestamp AT TIME ZONE "America/New_York") > 9) OR (EXTRACT(HOUR FROM timestamp AT TIME ZONE "America/New_York") = 9 AND EXTRACT(MINUTE FROM timestamp) >= 30))
        AND DATE(timestamp, "America/New_York")  = "{date_str}"
    )
    
    SELECT a.symbol, a.cur_date, a.daily_max, a.daily_min, ROUND(a.daily_avg, 2) as daily_avg, b.daily_close, ROUND(a.daily_volume / 1000000, 3) AS daily_volume_millions
    FROM (
        SELECT symbol, cur_date, MAX(close) AS daily_max, MIN(close) AS daily_min, AVG(close) AS daily_avg, SUM(volume) AS daily_volume
        FROM BASE
        WHERE TRUE
        group by symbol, cur_date
    ) a JOIN (
        SELECT a.symbol, a.cur_date, ROUND(AVG(close), 2) as daily_close
        FROM BASE a JOIN (
          SELECT symbol, cur_date, MAX(timestamp) AS last_timestamp
          FROM BASE
          GROUP BY symbol, cur_date
        ) b ON a.symbol = b.symbol AND a.cur_date = b.cur_date AND timestamp = b.last_timestamp
        GROUP BY a.symbol, a.cur_date # to prevent possible duplicates rows for the last timestamp.
    ) b ON a.symbol = b.symbol AND a.cur_date = b.cur_date 
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
    logging_util.info("query: ", query)
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
            'daily_close': row.daily_close,
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
