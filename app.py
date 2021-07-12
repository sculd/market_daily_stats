import logging
logging.basicConfig(level=logging.DEBUG)

from flask import Flask, request
import market_daily_stat

# make sure these libraries don't log debug statement which can contain
# sensitive information
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

app = Flask(__name__)

@app.route('/market_daily_stat', methods=['GET'])
def handle_market_daily_stat():
    date_str = None
    if request.args.get('date_str'):
        date_str = request.args.get('date_str')
    market_daily_stat.update_today_daily_stat(date_str=date_str)
    return 'done'

@app.route('/hello', methods=['GET'])
def hello():
    return 'hello world'

if __name__ == '__main__':
    # Used when running locally only. When deploying to Cloud Run,
    # a webserver process such as Gunicorn will serve the app.
    app.run(host='localhost', port=8081, debug=True)
