import MySQLdb


def connect_to_db():
    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='960423',
        db='1point3acres',
        charset="utf8"
    )
    return conn


def create_table(cursor, table_name):
    cursor.execute('create table if not exists ' + table_name + '''(
    url nvarchar(128),post_name nvarchar(128),author_url nvarchar(128),author_name nvarchar(128),
    reply int,pv int,date_time datetime,content text,PRIMARY KEY (url))''')


def insert_into_db(cursor, table_name, data_tuple):
    cursor.execute('insert into ' + table_name +
                   '''(url,post_name,author_url,author_name,reply,pv,date_time,content)
                   values (%s , %s , %s , %s , %s , %s , %s , %s)''', data_tuple)
