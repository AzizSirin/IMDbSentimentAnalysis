
from nltk.corpus import stopwords
from sqlite3 import Error
from multiprocessing import Pool
import time
from data_preparing import create_connection
import re
import spacy

REPLACE_NO_SPACE = re.compile("[.;:!\'<>£?,\"^+@#$½{}~´`%&/=_*(\-)\[\]]")
LONG_WORDS = re.compile(r'\W*\b\w{40,999}\b')
start_time = time.time()

conn = create_connection('2020-04-11')
c = conn.cursor()


def filling_the_database(review_data):  # Appending the cleaned reviews to same database
    try:
        c.executemany("""INSERT INTO preprocessed_reviews(review, review_score) VALUES (?, ?)""", review_data)
        conn.commit()
    except Error as e:
        print(e)


def review_cleaner(review):
    lemmatizer = spacy.load('en_core_web_sm', disable=['parser', 'ner'])
    bulk_data = []
    for each in review:
        review_temp = ''.join([i for i in each[0] if not i.isdigit()])
        review_temp = REPLACE_NO_SPACE.sub(" ", review_temp.lower())
        review_temp = lemmatizer(review_temp)
        review_temp = [token.lemma_ if token.lemma_ != '-PRON-' else '' for token in review_temp]
        review_temp = ' '.join([word for word in review_temp if word not in stopwords.words('english')])
        new_review_data = (review_temp, each[1])
        bulk_data.append(new_review_data)
    filling_the_database(bulk_data)


if __name__ == "__main__":

    review_data = ()
    bulk_data = []
    amount_of_reviews = 0
    previous_amount = 0
    conn = create_connection('2020-04-11')
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS preprocessed_reviews(review TEXT, review_score INTEGER, ID PRIMARY KEY)""")
    conn.commit()
    total_number_of_reviews = c.execute("""SELECT COUNT(*) FROM movie_reviews""")

    for each in total_number_of_reviews:
        total_number_of_reviews = each[0]

    while amount_of_reviews <= total_number_of_reviews:
        review_package = []
        amount_of_reviews += 100
        data = c.execute("""SELECT * FROM movie_reviews WHERE ID BETWEEN (?) AND (?)""", (previous_amount, amount_of_reviews-1))
        conn.commit()
        previous_amount = amount_of_reviews
        for each in data:
            review_data = (each[0], each[1])
            review_package.append(review_data)
            del review_data
        bulk_data.append(review_package)
        del review_package
        print(amount_of_reviews)

    p = Pool(4)
    p.map(review_cleaner, bulk_data)
    p.close()
    print('---- %s seconds ----' % (time.time() - start_time))
