import logging

from flask import Flask

from blueprints.worker_bp import worker_bp
from db import connection_pool

app = Flask(__name__)


logging.basicConfig(filename='db_logs.log', level=logging.INFO)
app.register_blueprint(worker_bp, url_prefix='/worker')

@app.route('/')
def hello_world():  # put application's code here
    return 'Hello World!'


if __name__ == '__main__':
    app.run()
