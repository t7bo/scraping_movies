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
                             
                             id INTEGER PRIMARY KEY AUTOINCREMENT,
                             url TEXT,
                             title TEXT,
                             original_title TEXT,
                             year TEXT,
                             public TEXT,
                             screening TEXT,
                             mark REAL,
                             marks_nb INTEGER,
                             category TEXT,
                             synopsis TEXT,
                             director TEXT,
                             budget INTEGER,
                             boxoffice INTEGER,
                             country TEXT,
                             casting TEXT,
                             poster URL,
                             imdb_id INTEGER
                             
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

                if title is None:
                    pass
                else:
                    title = title.title()
                    adapter['title'] = title

            elif field_name == 'original_title':

                original_title = adapter.get('original_title')

                if original_title is not None:
                    original_title = original_title[17:]
                    original_title = original_title.title()
                else:
                    pass
            
                adapter['original_title'] = original_title

            
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
                else:
                    adapter['public'] = public

            
            elif field_name == "screening":

                screening = adapter.get('screening')
                screen_split = screening.split(' ')
                hours = ''.join(filter(str.isdigit, screen_split[0]))
                minutes = ''.join(filter(str.isdigit, screen_split[1]))
                time = 0

                time = int(hours) * 60
                time = int(time) + int(minutes)
                adapter['screening'] = time


            elif field_name == "mark":
                mark = adapter.get('mark')
                if mark is not None:
                    mark = float(mark)
                    adapter['mark'] = mark
                
            elif field_name == "marks_nb":
                marks_nb = adapter.get('marks_nb')
                if marks_nb is not None:
                    if "M" in marks_nb:
                        marks_nb = float(''.join(filter(str.isdigit, marks_nb)))
                        marks_nb = marks_nb * 1000000
                    elif "K" in marks_nb:
                        marks_nb = float(''.join(filter(str.isdigit, marks_nb)))
                        marks_nb = marks_nb * 1000

                marks_nb = int(marks_nb)
                adapter['marks_nb'] = marks_nb


            elif field_name == "boxoffice":
                boxoffice = adapter.get('boxoffice')
                if boxoffice.isdigit() == False:
                    boxoffice = ''.join(filter(str.isdigit, boxoffice))
                adapter['boxoffice'] = int(boxoffice)

            elif field_name == "budget":
                budget = adapter.get('budget')
                if budget is not None:
                    if budget[0] == '¥':
                        budget = int(''.join(filter(str.isdigit, budget)) / 150)
                    elif budget[0] == '₩':
                        budget = int(''.join(filter(str.isdigit, budget)) * 0.00075)
                    elif budget[0] == '€':
                        budget = int(''.join(filter(str.isdigit, budget)) * 1.09)
                    elif budget[0] == 'A':
                        budget = int(''.join(filter(str.isdigit, budget)) * 0.65)
                    elif budget[0] == '£':
                        budget = int(''.join(filter(str.isdigit, budget)) * 1.27)
                    elif budget[0] == '₹':
                        budget = int(''.join(filter(str.isdigit, budget)) * 0.012)
                    elif budget[:2] == 'DE':
                        budget = int(''.join(filter(str.isdigit, budget)) * 0.55494846)
                    elif budget[:2] == 'DK':
                        budget = int(''.join(filter(str.isdigit, budget)) * 0.15)
                    elif budget[0] == "F":
                        budget = int(''.join(filter(str.isdigit, budget)) * 0.16)
                    elif budget[0] == "R":
                        budget = int(''.join(filter(str.isdigit, budget)) * 0.2)
                    else:
                        budget = int(''.join(filter(str.isdigit, budget)))
                adapter['budget'] = budget

            
            elif field_name == "category":
                category = adapter.get('category')
                if category is not None:
                    category = ', '.join(category)
                    category = category.replace("'", "").replace("[", "").replace("]", "")
                adapter['category'] = str(category)
            
            elif field_name == "country":
                country = adapter.get('country')
                if country is not None:
                    country = ', '.join(country)
                    country = country.replace("'", "").replace("[", "").replace("]", "")
                adapter['country'] = str(country)

            elif field_name == "casting":
                casting = adapter.get('casting')
                if casting is not None:
                    casting = set(casting)
                    casting = ', '.join(casting)
                    casting = casting.replace("'", "").replace("[", "").replace("]", "")
                adapter['casting'] = casting

            elif field_name == 'imdb_id':
                imdb_id = adapter.get('url')
                if imdb_id is not None:
                    imdb_id = imdb_id.split('/')[-2]
                adapter['imdb_id'] = imdb_id

        self.cur.execute("""
                         INSERT INTO movies (url, title, original_title, year, public, screening, mark, marks_nb, category, synopsis, director, budget, boxoffice, country, casting, poster, imdb_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                            adapter["category"],
                            adapter["synopsis"],
                            adapter["director"],
                            adapter["budget"],
                            adapter["boxoffice"],
                            adapter["country"],
                            adapter["casting"],
                            adapter["poster"],
                            adapter['imdb_id']
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

        field_names = adapter.field_names()

        for field_name in field_names:

            if field_name == 'url':
                pass

            elif field_name == 'title':
                pass

            elif field_name == "nb_seasons":
                years = adapter.get('years')
                nb_seasons = adapter.get('nb_seasons')
                if '–' not in years and nb_seasons is None:
                    nb_seasons = 1
                    adapter['nb_seasons'] = nb_seasons

            elif field_name == 'nb_seasons':
                nb_seasons = adapter.get('nb_seasons')
                if nb_seasons is not None:
                    adapter['nb_seasons'] = int(nb_seasons)

            elif field_name == "nb_episodes":
                nb_episodes = adapter.get('nb_episodes')
                if nb_episodes is not None:
                    adapter['nb_episodes'] = int(nb_episodes)

            elif field_name == "episode_length":
                episode_length = adapter.get('episode_length')
                nb_episodes = adapter.get('nb_episodes')
                if episode_length is not None:
                    if 'h' in episode_length:
                        split = episode_length.split(' ')
                        hours = ''.join(filter(str.isdigit, split[0]))
                        minutes = ''.join(filter(str.isdigit, split[1]))
                        episode_length = 0
                        episode_length = round((((int(hours) * 60) + int(minutes)) / int(nb_episodes)), 2)
                    else:
                        episode_length = ''.join(filter(str.isdigit, episode_length))

                adapter["episode_length"] = episode_length

            
            elif field_name == "mark":
                pass

            elif field_name == "marks_nb":
                marks_nb = adapter.get('marks_nb')
                if marks_nb is not None:
                    if "M" in marks_nb:
                        marks_nb = int(''.join(filter(str.isdigit, marks_nb))) * 1000000
                    elif "K" in marks_nb:
                        marks_nb = int(''.join(filter(str.isdigit, marks_nb))) * 1000
                    adapter["marks_nb"] = marks_nb

        
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