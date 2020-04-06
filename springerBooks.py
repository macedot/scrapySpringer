import logging
import scrapy
import requests
import csv
import html2text
import os
from pathlib import Path
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
        urls = self.books['OpenURL'][:3]
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
                title = title.replace(":","-")

        authors = "INVALID"
        list_authors = list()
        for book_authors in response.css('.authors__name'):
            this_author = book_authors.css('::text').get()
            this_author = converter.handle(this_author)
            this_author = this_author.strip("\n")
            list_authors.append(this_author)
        if len(list_authors) > 0:
            authors = ",".join(list_authors)

        url_pdf = ""
        for book_authors in response.css('.cta-button-container__item'):
            for href in book_authors.css('a ::attr(href)'):
                url_pdf = href.get()

        url_pdf = "https://link.springer.com/" + url_pdf
        pdf_size = 0
        pdf_file_name = "{}_{}.pdf".format(title, authors)

        books_path = Path("books")
        books_temp = Path("temp")
        pdf_tmp = books_temp / pdf_file_name
        pdf_final = books_path / pdf_file_name

        if os.path.exists(pdf_final):
            self.logger.debug("Skipping PDF: ({} -> {})".format(url_pdf, pdf_final))
            pass

        try:
            with requests.get(url_pdf) as pdf_file:
                pdf_size = len(pdf_file.content)
                with open(pdf_tmp, 'wb') as file:
                    file.write(pdf_file.content)
                os.rename(pdf_tmp, pdf_final)
        except IOError:
            self.logger.error("Unable to save PDF: ({} -> {})".format(url_pdf, pdf_tmp))
            pass

        yield {
            'idx': idx,
            'title': title,
            'authors': authors,
            'url': url,
            'url_pdf': url_pdf,
            'pdf': pdf_final,
            'pdf_size': pdf_size,
        }

if __name__ == '__main__':
    print(scrapy.__version__)
    books = read_csv('data/FreeEnglishTextbooks.csv')
    print(books.keys())
