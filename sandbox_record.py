import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

import record

item = {
    'symbol': 'dummy',
    'market': 'dummy',
    'cur_date': '2021-07-09',
    'daily_max': '11',
    'daily_min': '10',
    'daily_avg': '10',
    'daily_volume_millions': '10'
}

record.records([item])

