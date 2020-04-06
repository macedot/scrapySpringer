#!/usr/bin/python3
import csv
import os
from pathlib import Path
from collections import defaultdict
from typing import Dict

import requests
import scrapy
import html2text

converter = html2text.HTML2Text()
converter.ignore_links = True


def read_csv(input_file: str, delimiter: str = ',') -> Dict[str, list]:
    """Read file to dict with header as dict keys;"""
    columns = defaultdict(list)
    with open(input_file, encoding="utf8") as file:
        reader = csv.reader(file, delimiter=delimiter)
        headers = next(reader)
        column_nums = range(len(headers))  # Do NOT change to xrange
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
        urls = self.books['OpenURL']
        for idx in range(len(urls)):
            self.log('url = %s' % urls[idx])
            yield scrapy.Request(url=urls[idx],
                                 callback=self.parse,
                                 cb_kwargs={'idx': idx})

    def parse(self, response, idx):
        url = response.url
        self.logger.info("Parse ({})".format(url))

        title = "INVALID"
        page_title = response.css('.page-title')[0]
        if page_title:
            title = page_title.css('h2 ::text').get()
            if title is None:
                title = page_title.css('h1 ::text').get()
                title = title.replace(":", "-")

        authors = "INVALID"
        list_authors = list()
        for book_authors in response.css('.authors__name'):
            this_author = book_authors.css('::text').get()
            this_author = converter.handle(this_author)
            this_author = this_author.strip("\n")
            list_authors.append(this_author)
        if len(list_authors) > 0:
            authors = ",".join(list_authors)

        url_book = ""
        for book_authors in response.css('.cta-button-container__item'):
            for href in book_authors.css('a ::attr(href)'):
                url_book = href.get()

        url_book = "https://link.springer.com/" + url_book
        book_size = 0
        book_file_name = "{}_{}.pdf".format(title, authors)

        books_path = Path("books")
        books_temp = Path("temp")
        book_tmp = books_temp / book_file_name
        book_final = books_path / book_file_name

        if os.path.exists(book_final):
            self.logger.debug(
                "Skipping Book: ({} -> {})".format(url_book, book_final))
            return

        try:
            with requests.get(url_book) as book_file:
                book_size = len(book_file.content)
                with open(book_tmp, 'wb') as file:
                    file.write(book_file.content)
                os.rename(book_tmp, book_final)
        except IOError:
            self.logger.error(
                "Unable to save Book: ({} -> {})".format(url_book, book_tmp))
            return

        yield {
            'idx': idx,
            'title': title,
            'authors': authors,
            'url': url,
            'url_book': url_book,
            'book': book_final,
            'book_size': book_size,
        }


if __name__ == '__main__':
    print(scrapy.__version__)
    BOOK_LIST = read_csv('data/FreeEnglishTextbooks.csv')
    print(BOOK_LIST.keys())
