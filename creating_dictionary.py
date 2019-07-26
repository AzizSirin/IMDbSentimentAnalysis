from data_preparing import create_connection
from nltk.tokenize import word_tokenize
import sqlite3
import time

start_time = time.time()


def create_dictionary_database():
    connection = sqlite3.connect('dictionary.db', timeout=10000)
    c = connection.cursor()
    c.execute("""CREATE TABLE IF NOT EXIST dictionary (word TEXT)""")
    connection.close()


def fetch():

    content = ''

    conn = create_connection("preprocessed_data")
    c = conn.cursor()
    data = c.execute("""SELECT review FROM movie_reviews""")

    for review in data:
        content = content + ' ' + review[0]
    content = content.split()
    dictionary = set(content)
    conn.close()

    create_dictionary_database()

    connection = create_connection("dictionary")
    cur = connection.cursor()

    for word in dictionary:
        cur.execute("""INSERT INTO dictionary (word) VALUES (?)""", (word))

    connection.close()

if __name__ == "__main__":

    fetch()
    print("--- %s seconds ---" % (time.time() - start_time))