import sqlite3
from sqlite3 import Error
from multiprocessing import Pool
import time

start_time = time.time()


def create_database(name):

    connection = sqlite3.connect('{}.db'.format(name), timeout=50)
    c = connection.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS movie_reviews (review TEXT)""")
    return connection


def create_connection(db_file):

    try:
        conn = sqlite3.connect(db_file)
        return conn

    except Error as e:
        print(e)

    return None


def fetch_from_database(score):
    score = int(score)

    conn = create_connection('2019-06.db')
    c = conn.cursor()
    data = c.execute("""SELECT * FROM movie_reviews WHERE review_score=? LIMIT 3000""", (score,))

    score = str(score)
    connection_of_new_database = create_database(score)
    cursor = connection_of_new_database.cursor()

    for each in data:

        cursor.execute("""INSERT INTO movie_reviews (review) VALUES (?)""", (each[0],))
        connection_of_new_database.commit()

    connection_of_new_database.close()


if __name__ == "__main__":

    scores = [1,2,3,4,5,6,7,8,9,10]

    p = Pool(10)
    p.map(fetch_from_database, scores)

    print("--- %s seconds---" % (time.time() - start_time))