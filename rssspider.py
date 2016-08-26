# -*- coding: utf-8 -*-
import scrapy
import csv

def get_urls_from_csv():
    with open('data.csv', newline='') as csv_file:
        data = csv.reader(csv_file, delimiter=',')
        scrapurls = []
        for row in data:
             yield row[2]

class rssitem(scrapy.Item):
    sourceurl = scrapy.Field()
    rssurl = scrapy.Field()


class RssparserSpider(scrapy.Spider):
    name = "rssspider"
    allowed_domains = ["*"]
    start_urls = ()

    def start_requests(self):
        for start_url in get_urls_from_csv():
            yield scrapy.http.Request(url="http://{}".format(start_url))


    def parse(self, response):
        res = response.xpath('//link[@type="application/rss+xml"]/@href')
        for sel in res:
            item = rssitem()
            item['sourceurl']=response.url
            item['rssurl']=response.urljoin(sel.extract())
            yield item
        pass
