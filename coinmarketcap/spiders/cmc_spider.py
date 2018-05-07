# -*- coding: utf-8 -*-
import scrapy
import json
from datetime import datetime
import logging
from unidecode import unidecode

class CoinMarketItem(scrapy.Item):
    reference_date = scrapy.Field()

class CmcSpiderSpider(scrapy.Spider):
    name = 'cmc_spider'
    start_url = 'https://coinmarketcap.com'
    today = datetime.today()

    def start_requests(self):
        yield scrapy.Request(self.start_url,
                             callback = self.parse_coins,
                             errback = self.http_error
                            )

    def parse_coins(self, response):
        coin_links = response.css('.currency-name-container::attr(href)').extract()
        for coin in coin_links[:1]: # REMOVE LIMIT
            yield scrapy.Request(start_url + coin + '#parse_markets',
                                 callback = self.parse_markets,
                                 errback = self.http_error)


    def parse_markets(self, response):
        markets = response.css('tbody tr')

        for market in markets:
            item = CoinMarketItem()
            item['reference_date'] = self.today
            item['name'] = market.css('td::attr(data-sort)').extract_first()

            # CONTINUE HERE!
            yield item

        # item = CoinMarketItem()
        # item['reference_date'] = datetime.today()
        # yield item

    def http_error(self, failure):
        logging.error(repr(failure))

        if failure.check(HttpError):
            response = failure.value.response
            logging.error('HttpError on {}'.format(response.url))

        elif failure.check(DNSLookupError):
            # this is the original request
            request = failure.request
            logging.error('DNSLookupError on {}'.format(request.url))

        elif failure.check(TimeoutError):
            request = failure.request
            logging.error('TimeoutError on {}'.format(request.url))