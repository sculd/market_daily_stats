import boto3
from botocore.config import Config

my_config = Config(
    region_name = 'us-east-2',
    signature_version = 'v4',
    retries = {
        'max_attempts': 1,
        'mode': 'standard'
    }
)

import logging_util

_RESOURCE_DYNAMODB = 'dynamodb'
_TABLE_NAME = 'market_daily_stat'

_dynamodb = boto3.resource(_RESOURCE_DYNAMODB, config=my_config)
_table = _dynamodb.Table(_TABLE_NAME)


def records(items):
    '''
    Batch writes the given items.

    :param items: a list whose elements are dictionaries with keys:
    symbol, market, date_str,m daily_max, daily_min, daily_avg, daily_volumne_millions.
    :return:
    '''
    try:
        with _table.batch_writer() as batch:
            for item in items:
                message_str = '[dynamodb.record] {}'.format(item)
                logging_util.info(message_str)
                batch.put_item(
                    Item={
                        'symbol': item['symbol'],
                        'market': item['market'],
                        'date_str': item['cur_date'],
                        'daily_max': str(item['daily_max']),
                        'daily_min': str(item['daily_min']),
                        'daily_avg': str(item['daily_avg']),
                        'daily_close': str(item['daily_close']),
                        'daily_volume_millions': str(item['daily_volume_millions'])
                    }
                )
    except Exception as ex:
        logging_util.warning('an exception occurred while writing to dynamodb: {e}'.format(e=str(ex)))
