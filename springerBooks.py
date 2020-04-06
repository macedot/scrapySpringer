import logging
import scrapy
import requests
import csv
import html2text
from collections import defaultdict
from typing import Dict, List

converter = html2text.HTML2Text()
converter.ignore_links = True

def read_csv(input_file: str, delimiter: str = ',') -> Dict[str, list]:
    """Read file to dict with header as dict keys;"""
    columns = defaultdict(list)
    with open(input_file, encoding="utf8") as f:
        reader = csv.reader(f, delimiter=delimiter)
        headers = next(reader)
        column_nums = range(len(headers)) # Do NOT change to xrange
        for row in reader:
            for i in column_nums:
                columns[headers[i]].append(row[i])
    # Following line is only necessary if you want a key error for invalid column names
    columns = dict(columns)
    return columns

class BookSpider(scrapy.Spider):
    name = 'springerBooks'
    books = defaultdict(list)

    def start_requests(self):
        self.books = read_csv('data/FreeEnglishTextbooks.csv')
        urls = self.books['OpenURL'][:5]
        for idx in range(len(urls)):
            self.log('url = %s' % urls[idx])
            yield scrapy.Request(url=urls[idx],
                                 callback=self.parse,
                                 cb_kwargs={'idx': idx})

    def parse(self, response):
        idx = response.cb_kwargs['idx']
        url = response.url
        self.logger.info("Parse ({})".format(url))

        title = "INVALID"
        authors = "INVALID"

        page_title = response.css('.page-title')[0]
        if page_title:
            title = page_title.css('h2 ::text').get()
            if title is None:
                title = page_title.css('h1 ::text').get()
                title = title.replace(":","-")

        list_authors = list()
        for book_authors in response.css('.authors__name'):
            this_author = book_authors.css('::text').get()
            this_author = converter.handle(this_author)
            this_author = this_author.strip("\n")
            list_authors.append(this_author)
        authors = ",".join(list_authors)

        pdf = "{}_{}.pdf".format(title, authors)
        yield {
            'idx': idx,
            'title': title,
            'authors': authors,
            'url': url,
            'pdf': pdf,
        }

        # with open(filename, 'wb') as f:
        #     f.write(response.body)
        # self.log('Saved file %s' % filename)

        # for title in response.css('.post-header>h2'):
        #     yield {'title': title.css('a ::text').get()}

        # for next_page in response.css('a.next-posts-link'):
        #     yield response.follow(next_page, self.parse)

        # url = 'https://www.python.org/static/img/python-logo@2x.png'
        # myfile = requests.get(url)
        # open('books/PythonImage.png', 'wb').write(myfile.content)


if __name__ == '__main__':
    print(scrapy.__version__)
    books = read_csv('data/FreeEnglishTextbooks.csv')
    print(books.keys())
