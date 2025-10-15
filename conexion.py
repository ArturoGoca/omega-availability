import pymysql

def get_conn():
    return pymysql.connect(
        host="127.0.0.1",
        user="root",
        password="",
        database="omega",
        cursorclass=pymysql.cursors.DictCursor,
        charset="utf8mb4",
        autocommit=True,
    )