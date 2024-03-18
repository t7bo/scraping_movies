# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import sqlite3


# MOVIES

class MoviescraperPipeline:
    def __init__(self) -> None:
        # create database
        self.con = sqlite3.connect('../data/database/imdb.db')
        # create cursorto execute commands
        self.cur = self.con.cursor()
        # create table
        self.cur.execute("""
                         CREATE TABLE IF NOT EXISTS movies(
                             
                             url TEXT,
                             title TEXT,
                             original_title TEXT,
                             year TEXT,
                             public TEXT,
                             screening TEXT,
                             mark TEXT,
                             marks_nb TEXT,
                             popularity TEXT,
                             category LIST,
                             synopsis TEXT,
                             director TEXT,
                             budget TEXT,
                             boxoffice TEXT,
                             country LIST,
                             casting TEXT
                             
                             )
                        """)
        

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        field_names = adapter.field_names()

        for field_name in field_names:

            if field_name == 'url':
                pass

            # ADD A CAPITAL LETTER TO EACH WORD COMPOSING TITLES & ORIGINAL TITLES
            # CAREFUL WITH NAN
            elif field_name == 'title':

                title = adapter.get('title')
                original_title = adapter.get('original_title')

                if title is None:
                    pass
                else:
                    title = title.title()
                    adapter['title'] = title

            elif field_name == 'original_title':

                original_title = adapter.get('original_title')

                if original_title is not None:
                    if "Titre original\xa0: " in original_title:
                        original_title = original_title[17:].title() # supprimer "Titre Original :"
                    adapter['original_title'] = original_title.title()

            
            elif field_name == 'year':

                string = adapter.get('year')

                if string.isdigit() != True:
                    # remove str characters
                    # then, convert to int
                    for char in string:
                        if char.isdigit() == False:
                            string = string.remove(char)
                    adapter['year'] = int(string)
                else:
                    string = int(string)
                    adapter['year'] = string


            elif field_name == 'public':

                public = adapter.get('public')

                if public is None:
                    adapter['public'] = 'NaN'

                if public.isdigit() == True:
                    public = "-" + public
                    adapter['public'] = public
                else:
                    adapter['public'] = public

        
        self.cur.execute("""
                         INSERT INTO movies (url, title, original_title, year, public, screening, mark, marks_nb, popularity, category, synopsis, director, budget, boxoffice, country, casting) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                            adapter["country"],
                            adapter["casting"],
                         ))
        
        # execute insert of data into the database
        self.con.commit()

        return item
    
    def close_spider(self, spider):
        # close cursor & connection to db
        self.cur.close()
        self.con.close()
    
    








# SERIES
            
class SeriescraperPipeline:
    def __init__(self) -> None:
        # create database
        self.con = sqlite3.connect('../data/database/imdb.db')
        # create cursorto execute commands
        self.cur = self.con.cursor()
        # create table
        self.cur.execute("""
                         CREATE TABLE IF NOT EXISTS series(
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
                         INSERT INTO series (url, title, years, nb_seasons, nb_episodes, episode_length, synopsis, mark, marks_nb) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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