# -*- coding: utf-8 -*-
import scrapy
import json
from datetime import datetime
import logging
import re

class CoinMarketItem(scrapy.Item):
    reference_date = scrapy.Field()
    main_currency_name = scrapy.Field()
    main_currency = scrapy.Field()
    exchange_name = scrapy.Field()
    source_currency = scrapy.Field()
    target_currency = scrapy.Field()
    volume_traded_24h = scrapy.Field()
    price = scrapy.Field()
    percentage_of_trades = scrapy.Field()

class CmcSpider(scrapy.Spider):
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
        for coin in coin_links:
            yield scrapy.Request(self.start_url + coin,
                                 callback = self.parse_markets,
                                 errback = self.http_error)


    def parse_markets(self, response):
        currency_raw = response.css('small.hidden-xs::text').extract_first()
        currency_ac = re.match('\((.*)\)', currency_raw).group(1)
        main_currency_name = response.css('h1 img::attr(alt)').extract_first()

        markets = response.css('tbody tr')
        market_data = [m.css('td::attr(data-sort)').extract() for m in markets]
         
        for market in market_data:
            item = CoinMarketItem()
            pair = market[1].split('/')
            item['main_currency'] = currency_ac
            item['main_currency_name'] = main_currency_name
            item['reference_date'] = self.today
            item['exchange_name'] = market[0]
            item['source_currency'] = pair[0]
            item['target_currency'] = pair[1]
            item['volume_traded_24h'] = float(market[2])
            item['price'] = float(market[3])
            item['percentage_of_trades'] = float(market[4])

            yield item

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