import datetime
import pytz
_TIMEZONE_US_EAST = pytz.timezone('US/EASTERN')


def get_now_et():
    return pytz.utc.localize(datetime.datetime.utcnow()).astimezone(_TIMEZONE_US_EAST)


def get_today_date_str():
    return get_now_et().strftime('%Y-%m-%d')

