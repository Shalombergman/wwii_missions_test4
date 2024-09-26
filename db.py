import psycopg2

from psycopg2 import pool

import logging


logging.basicConfig(filename='db_logs.log', level=logging.INFO)

def get_db_connection():
    if connection_pool:
        conn = connection_pool.getconn()
        return conn

def release_db_connection(conn):
    connection_pool.putconn(conn)





connection_pool  = psycopg2.pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dbname="wwii_missions",
    user="shalom_bergman",
    password="1234",
    host="localhost",
    port="5432"
)

