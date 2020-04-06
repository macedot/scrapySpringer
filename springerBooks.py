#!/usr/bin/python3
import os
import re
import csv

from pathlib import Path
from typing import Dict
from urllib.parse import urlparse
from collections import defaultdict

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
    books_path = Path("books")
    temp_path = Path("temp")

    def start_requests(self):
        self.books = read_csv('data/FreeEnglishTextbooks.csv')
        urls = self.books['OpenURL'][:1]
        for idx in range(len(urls)):
            self.log('url = %s' % urls[idx])
            yield scrapy.Request(url=urls[idx],
                                 callback=self.parse,
                                 cb_kwargs={'idx': idx})

    def parse(self, response, idx):
        url = response.url
        try:
            self.logger.info("Parse ({})".format(url))

            title = self.books['Book Title'][idx]
            authors = self.books['Author'][idx]
            parsed_uri = urlparse(response.url)

            url_book = ""
            for book_authors in response.css('.cta-button-container__item'):
                with book_authors.css('a ::attr(href)') as href:
                    url_book = href[0].get()

            if len(url_book) == 0:
                self.logger.error(
                    "Unable to find Book URL at '{}}'".format(url))
                return

            base_uri = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)
            url_book = base_uri + url_book

            # if os.path.exists(book_final):
            #     self.logger.debug(
            #         "Skipping Book: ({}, {} -> {})".format(idx, url_book, book_final))
            #     return
        except:
            self.logger.error(
                "Error while reading Book URL: ({}, {})'".format(idx, url))
            return

        try:
            with requests.get(url_book) as book_request:
                if book_file.status_code != 200:
                    return
                content_disposition = book_request.headers['content-disposition']
                book_size = len(book_file.content)
                if book_size <= 0:
                    return
                fname = re.findall("filename=(.+)", content_disposition)[0]
                book_tmp = self.temp_path / fname
                with open(book_tmp, 'wb') as file:
                    file.write(book_file.content)
                book_fname = "{}_{}_{}".format(title, authors, fname)
                book_final = self.books_path / book_fname
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
