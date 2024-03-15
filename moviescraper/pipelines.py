# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import sqlite3

class MoviescraperPipeline:
    def __init__(self) -> None:
        # create database
        self.con = sqlite3.connect('moviesdb.db')
        # create cursorto execute commands
        self.cur = self.con.cursor()
        # create table
        self.cur.execute("""
                         CREATE TABLE IF NOT EXISTS moviesdb(
                             
                             url TEXT,
                             title TEXT,
                             original_title TEXT,
                             year TEXT,
                             public TEXT,
                             screening TEXT,
                             mark TEXT,
                             marks_nb TEXT,
                             popularity TEXT,
                             category TEXT,
                             synopsis TEXT,
                             director TEXT,
                             budget TEXT,
                             boxoffice TEXT
                             
                         )
                         """)
        
    
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        self.cur.execute("""
                         INSERT INTO moviesdb (url, title, original_title, year, public, screening, mark, marks_nb, popularity, category, synopsis, director, budget, boxoffice) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                         """,
                         (
                            adapter["url"],
                            adapter["title"],
                            adapter["original_title"],
                            adapter["year"],
                            adapter["public"],
                            adapter["screening"],
                            adapter["mark"],
                            adapter["marks_nb"],
                            adapter["popularity"],
                            adapter["category"],
                            adapter["synopsis"],
                            adapter["director"],
                            adapter["budget"],
                            adapter["boxoffice"],
                         ))
        
        # execute insert of data into the database
        self.con.commit()

        return item
    
    def close_spider(self, spider):
        # close cursor & connection to db
        self.cur.close()
        self.con.close()
    
    
class SeriescraperPipeline:
    def __init__(self) -> None:
        # create database
        self.con = sqlite3.connect('seriesdb.db')
        # create cursorto execute commands
        self.cur = self.con.cursor()
        # create table
        self.cur.execute("""
                         CREATE TABLE IF NOT EXISTS seriesdb(
                             url TEXT,
                             title TEXT,
                             years TEXT,
                             nb_seasons TEXT,
                             nb_episodes TEXT,
                             episode_length TEXT,
                             synopsis TEXT,
                             mark TEXT,
                             marks_nb TEXT
                         )
                         """)
        
    
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        self.cur.execute("""
                         INSERT INTO seriesdb (url, title, years, nb_seasons, nb_episodes, episode_length, synopsis, mark, marks_nb) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                         """,
                         (
                             
                         adapter["url"],
                         adapter["title"],
                         adapter["years"],
                         adapter["nb_seasons"],
                         adapter["nb_episodes"],
                         adapter["episode_length"],
                         adapter['synopsis'],
                         adapter["mark"],
                         adapter["marks_nb"],
                         
                         ))
        
        # execute insert of data into the database
        self.con.commit()

        return item
    
    def close_spider(self, spider):
        # close cursor & connection to db
        self.cur.close()
        self.con.close()