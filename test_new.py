from multiprocessing import Pool
import time
from requests import get
from bs4 import BeautifulSoup
import sqlite3

time_frame = '2020-04-11'
connection = sqlite3.connect('{}.db'.format(time_frame), timeout=10000)
c = connection.cursor()


def create_table():  # Creating the database to store reviews and scores

    c.execute("""CREATE TABLE IF NOT EXISTS movie_reviews
    (review TEXT, review_score INTEGER)""")


def movie_link_scraper(path):  # Getting the movies from IMDb list.

    # You need to configure here if you want to scrape movies from other lists, it should get title path.

    movie_list = []

    page_count = 1

    length = 5000

    movie_count = 0

    while page_count < 50:

        link = path + '?page=' + str(page_count)
        time.sleep(4)
        page = get(link)

        page_soup = BeautifulSoup(page.text, 'html.parser')

        printProgressBar(0, length, prefix='Progress:', suffix='Complete', length=50)

        for each in page_soup.findAll('h3', class_='lister-item-header'):
            movie_count += 1
            each = each.find('a')['href']
            movie_list.append(each)
            printProgressBar(movie_count + 1, length, prefix='Progress:', suffix='Complete', length=50)
        page_count += 1

    print(movie_count, 'movies listed.')
    time.sleep(5)
    return movie_list


def review_and_score_scraper(each):

    review_id = 0
    print(each)

    review_data = ()
    bulk_data = []

    movie_link = get('https://www.imdb.com' + each + 'reviews?ref_=tt_urv')
    soup_link = BeautifulSoup(movie_link.text, 'html.parser')

    for review in soup_link.findAll('div', class_='lister-item-content'):

        if review.find('div', class_="ipl-ratings-bar") is not None:  # If there is no score, we don't need the review

            review_text = review.find('div', class_="text show-more__control").text  # Scraping the review
            review_text = review_text.replace("\n", " ")

            review_score = review.find('span', class_='').text  # Scraping the score
            review_score = int(review_score)

            review_data = (review_text, review_score)
            bulk_data.append(review_data)
            del review_data

            review_id += 1
            if (review_id % 1000) == 0:
                print(review_id, 'reviews of', each, 'have been fetched...')
                time.sleep(5)

    if soup_link.find('div', class_='load-more-data') is not None:  # If there are more review page, we're getting the key of next page

        key = soup_link.find('div', class_='load-more-data')['data-key']

        while key is not None:  # And if there are more reviews, scraping them here...


            next_page = get('https://www.imdb.com' + each + 'reviews/_ajax?ref_=undefined&paginationKey=' + key)
            next_page_soup = BeautifulSoup(next_page.text, 'html.parser')

            for next_page_review in next_page_soup.findAll('div', class_='lister-item-content'):

                if next_page_review.find('div', class_='ipl-ratings-bar') is not None:

                    review_text = next_page_review.find('div', class_='text show-more__control').text
                    review_text = review_text.replace("\n", " ")

                    review_score = next_page_review.find('span', class_='').text
                    review_score = int(review_score)

                    review_data = (review_text, review_score)
                    bulk_data.append(review_data)
                    del review_data

                    review_id += 1
                    if (review_id % 1000) == 0:
                        print(review_id, 'reviews of', each, 'have been fetched...')
                        time.sleep(5)

            try:  # In the end of every page we need to check if there is more. So we can keep continue...
                key = next_page_soup.find('div', class_='load-more-data')['data-key']
            except:
                key = None

    else:
        key = None

    print('Total of', review_id, 'reviews of', each, 'have been fetched...')

    # Appending them to database
    c.executemany("""INSERT INTO movie_reviews VALUES (?, ?)""", bulk_data)
    connection.commit()


def printProgressBar (iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█', printEnd="\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end=printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()


if __name__ == "__main__":

    movie_list = movie_link_scraper('https://www.imdb.com/list/ls057823854/')

    create_table()
    p = Pool(25)
    p.map(review_and_score_scraper, movie_list)
    p.terminate()

