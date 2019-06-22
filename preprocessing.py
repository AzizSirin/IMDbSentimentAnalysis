import sqlite3
from sqlite3 import Error
from multiprocessing import Pool
import time
from data_preparing import create_database
from data_preparing import create_connection
import re

REPLACE_NO_SPACE = re.compile("[.;:!\'?,\"(\-_)\[\]]")

"""def create_connection(db_file):

    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)"""


def filling_the_database(review_data):
    try:
        conn = sqlite3.connect("preprocessed_data.db", timeout=100)
        c = conn.cursor()
        c.execute("""INSERT INTO movie_reviews (review, review_score) VALUES (?, ?)""", (review_data[0], review_data[1]))
    except Error as e:
        print(e)


def cleaner(review_data):
    for each in review_data:
        cleaned_review = [REPLACE_NO_SPACE.sub("", line.lower()) for line in each[0]]


def main(path):

    conn = create_connection(path)
    c = conn.cursor()
    data = c.execute("""SELECT * FROM moview_reviews""")

    p = Pool(4)
    p.map(filling_the_database, data)

if __name__ == "__main__":
    scores = [1,2,3,4,5,6,7,8,9,10]
    create_database("preprocessed_data")

    p = Pool(10)
    p.map(main, scores)
