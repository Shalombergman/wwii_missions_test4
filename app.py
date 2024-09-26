import logging

from flask import Flask

from db import connection_pool

app = Flask(__name__)


logging.basicConfig(filename='db_logs.log', level=logging.INFO)

@app.route('/')
def hello_world():  # put application's code here
    return 'Hello World!'


if __name__ == '__main__':
    app.run()
