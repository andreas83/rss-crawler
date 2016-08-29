# -*- coding: utf-8 -*-
import scrapy
import csv
import psycopg2
import feedparser
from langdetect import detect



def get_urls_from_csv():
    with open('/var/www/is.codejungle.org/rss/results/res.csv', newline='') as csv_file:
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
        sql_string = "INSERT INTO api_rsssource (name,description,image,link, language, created_date, modified_date) VALUES (%s,%s,%s,%s,%s, now(), now()) RETURNING id"
        img="";
        if hasattr(f.feed, "image"):
            img=f.feed.image

        lang=detect(f.feed.description)

        try:
            cursor.execute(sql_string, (f.feed.title, f.feed.description, img, f.feed.link, lang ))
        except Exception as e:
            print(e)
            pass
        self.conn.commit()
        nid = cursor.fetchone()[0]
        print(nid)
        for item in f.entries:
            sql_string = 'INSERT INTO api_rssitem (title, description, author, link, "RSSSource_id", created_date, modified_date) VALUES (%s,%s,%s,%s,%s,now(), now())';
            author="";
            if hasattr(item, "author"):
                author=item.author
            try:
                cursor.execute(sql_string, (item.title, item.description, author, item.link, nid))
            except Exception as e:
                print(e)
                pass
        self.conn.commit()
