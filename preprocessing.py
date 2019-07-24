import sqlite3
from sqlite3 import Error
from multiprocessing import Pool
import time
from data_preparing import create_database
from data_preparing import create_connection
import re

REPLACE_NO_SPACE = re.compile("[.;:!\'?,\"(\-_)\[\]/]")
LONG_WORDS = re.compile(r'\W*\b\w{40,999}\b')

def filling_the_database(review_data):  # 
    try:
        conn = create_connection('preprocessed_data')
        c = conn.cursor()
        c.execute("""INSERT INTO movie_reviews (review, review_score) VALUES (?, ?)""", (review_data[0], review_data[1]))
        conn.commit()
    except Error as e:
        print(e)


def cleaner(review_data):

    for each in review_data:
        cleaned_review = [REPLACE_NO_SPACE.sub(" ", each[0].lower())]
        cleaned_review = LONG_WORDS.sub("", cleaned_review[0])
        cleaned_review = ''.join([i for i in cleaned_review if not i.isdigit()])
        review_and_score = [cleaned_review, each[1]]
        filling_the_database(review_and_score)


def main(path):

    conn = create_connection(path)
    c = conn.cursor()
    data = c.execute("""SELECT * FROM movie_reviews LIMIT 2""")

    for each in data:
        cleaner(data)

if __name__ == "__main__":
    scores = [1,2,3,4,5,6,7,8,9,10]
    c = create_database("preprocessed_data")
    c.close()
    p = Pool(10)
    p.map(main, scores)
