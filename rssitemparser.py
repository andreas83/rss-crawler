# -*- coding: utf-8 -*-
import scrapy
import csv
import psycopg2
import feedparser
import datetime
from langdetect import detect
import pprint


def get_urls_from_csv():
    with open('res.csv', newline='') as csv_file:
        data = csv.reader(csv_file, delimiter=',')
        scrapurls = []
        for row in data:
             yield row[1]


class rssitemparser(scrapy.Spider):
    allowed_domains = ["*"]
    start_urls = ()
    name="rssitemparser"

    def start_requests(self):
        for start_url in get_urls_from_csv():
            if(start_url=="rssurl"):
                continue
            yield scrapy.http.Request(url=start_url)

    def __init__(self):
        try:
            print("test")
            self.conn=psycopg2.connect(dbname='rssly', user='rssly', password='data4me', host='127.0.0.1')
        except:
            print ("I am unable to connect to the database.")
    

    def parse(self, response):
        f = feedparser.parse(response.body)
        cursor = self.conn.cursor()
        sql_string = "INSERT INTO api_rsssource (name,description,image,link, language, created_date, modified_date) VALUES (%s,%s,%s,%s,%s, now(), %s) RETURNING id"


        title=f.feed.get("title", "")
        img=f.feed.get("image", "")
        desc=f.feed.get("description", "");
        link=f.feed.get("link", "");
        pub=f.feed.get("published", datetime.datetime.now())
        lang=detect(title)

        if(isinstance(img, feedparser.FeedParserDict)):
            img=img.href


        try:
            cursor.execute(sql_string, (title, desc, img, link, lang, pub))
        except Exception as e:
            print(e)
            pass
        self.conn.commit()
        nid = cursor.fetchone()[0]

        for item in f.entries:
            sql_string = 'INSERT INTO api_rssitem (title, description, author, link, "RSSSource_id", created_date, modified_date) VALUES (%s,%s,%s,%s,%s,now(), %s) RETURNING id';
            author=item.get("author", "")
            desc=item.get("description", "")
            pub=item.get("pubDate", datetime.datetime.now())
            enclosures=item.get("enclosures", 0)

            try:
                cursor.execute(sql_string, (item.title, desc, author, item.link, nid, pub))
            except Exception as e:
                print(e)
                pass
            self.conn.commit()
            itemid=cursor.fetchone()[0]

            for img in enclosures:
                sql='INSERT INTO api_rssitemresource (link, mime, "RSSItem_id", created_date, modified_date) VALUES (%s, %s,%s,now(),now())'
                cursor.execute(sql, (img.href, img.type, itemid))
