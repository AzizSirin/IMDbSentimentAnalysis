from multiprocessing import Pool
import time
from requests import get
from bs4 import BeautifulSoup
import sqlite3

time_frame = '2019-06'
connection = sqlite3.connect('{}.db'.format(time_frame), timeout=1000)
c = connection.cursor()


def create_table():  # Creating the database to store reviews and scores

    c.execute("""CREATE TABLE IF NOT EXISTS movie_reviews
    (review TEXT, review_score INTEGER)""")


def movie_scraper(toppath, bottompath):  # Getting the movies from top and bottom list of IMDb

    # You need to configure here if you want to scrape movies from other lists

    top_links = []
    bottom_links = []

    top_list = get(toppath)
    bottom_list = get(bottompath)

    soup_top = BeautifulSoup(top_list.text, 'html.parser')
    soup_bottom = BeautifulSoup(bottom_list.text, 'html.parser')

    for each in soup_top.findAll('td', class_='titleColumn'):
        each = each.find('a')['href']
        top_links.append(each)

    for each in soup_bottom.findAll('td', class_='titleColumn'):
        each = each.find('a')['href']
        bottom_links.append(each)

    scraping_it(top_links,bottom_links)


def scraping_it(top_links, bottom_links):  # Scraping the reviews and scores of each movie
    create_table()

    p = Pool(25)
    p.map(review_and_score_scraper, top_links)

    time.sleep(30)

    b = Pool(25)
    b.map(review_and_score_scraper, bottom_links)

    c.close()


def review_and_score_scraper(each):

    review_id = 0
    print(each)

    movie_link = get('https://www.imdb.com' + each + 'reviews?ref_=tt_urv')
    soup_link = BeautifulSoup(movie_link.text, 'html.parser')

    for review in soup_link.findAll('div', class_='lister-item-content'):

        if review.find('div', class_="ipl-ratings-bar") is not None:  # If there is no score, we don't need the review

            review_text = review.find('div', class_="text show-more__control").text  # Scraping the review
            review_text = review_text.replace("\n", " ")

            review_score = review.find('span', class_='').text  # Scraping the score
            review_score = int(review_score)

            # Appending them to database
            c.execute("""INSERT INTO movie_reviews VALUES (?, ?)""", (review_text, review_score))
            connection.commit()

            review_id += 1
            if (review_id % 1000) == 0:
                print(review_id)
                time.sleep(5)

    if soup_link.find('div', class_='load-more-data') is not None:  # If there are more review page, we're getting the key of next page

        key = soup_link.find('div', class_='load-more-data')['data-key']

        while key is not None:  # And if there are more reviews, scraping them here...

            print(key)

            next_page = get('https://www.imdb.com' + each + 'reviews/_ajax?ref_=undefined&paginationKey=' + key)
            next_page_soup = BeautifulSoup(next_page.text, 'html.parser')

            for next_page_review in next_page_soup.findAll('div', class_='lister-item-content'):

                if next_page_review.find('div', class_='ipl-ratings-bar') is not None:

                    review_text = next_page_review.find('div', class_='text show-more__control').text
                    review_text = review_text.replace("\n", " ")

                    review_score = next_page_review.find('span', class_='').text
                    review_score = int(review_score)

                    c.execute("""INSERT INTO movie_reviews VALUES (?, ?)""", (review_text, review_score))
                    connection.commit()
                    review_id += 1
                    if (review_id % 1000) == 0:
                        print(review_id)
                        time.sleep(5)

            try:  # In the end of every page we need to check if there is more. So we can keep continue...
                key = next_page_soup.find('div', class_='load-more-data')['data-key']
            except:
                key = None

    else:
        key = None


if __name__ == "__main__":

    scrape = movie_scraper('https://www.imdb.com/chart/top?ref_=nv_mv_250', 'https://www.imdb.com/chart/bottom')
    c.close()
