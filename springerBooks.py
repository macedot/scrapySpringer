import scrapy
import requests
import csv
from collections import defaultdict
from typing import Dict, List

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
        urls = self.books['OpenURL'][:10]
        for idx in range(len(urls)):
            self.log('url = %s' % urls[idx])
            yield scrapy.Request(url=urls[idx], 
                                 callback=self.parse, 
                                 cb_kwargs={'idx': idx})

    def parse(self, response, idx):
        page = response.url.split("/")[-2]
        filename = 'quotes-%s.html' % page
        self.log("Parse ({}, {}, {})".format(idx, page, filename))

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
    books = read_csv('data/FreeEnglishTextbooks.csv')
    print(books.keys())
