# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import sqlite3
from loguru import logger

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
                             screening INTEGER,
                             mark REAL,
                             marks_nb INTEGER,
                             category TEXT,
                             synopsis TEXT,
                             director TEXT,
                             budget INTEGER,
                             boxoffice INTEGER,
                             country TEXT,
                             casting TEXT,
                             poster TEXT
                             
                             )
                        """)
        
    @logger.catch #permet de colorer les logs pour + de visibilité
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        # ADD A CAPITAL LETTER TO EACH WORD COMPOSING TITLES & ORIGINAL TITLES 
        # -> title, original_title
        def title_each_word_of_str(string):
            return string.title()

        field_names = adapter.field_names()

        for field_name in field_names:

            # ADD A CAPITAL LETTER TO EACH WORD COMPOSING TITLES & ORIGINAL TITLES
            if field_name == 'title':
                title = adapter.get('title')
                if title is not None:
                    title = title_each_word_of_str(title)
                adapter['title'] = title
                    
            # ERASE "ORIGINAL TITLE :" FROM STR
            # ADD A CAPITAL LETTER TO EACH WORD COMPOSING TITLES & ORIGINAL TITLES
            elif field_name == 'original_title':
                original_title = adapter.get('original_title')
                if original_title is not None:
                    original_title = title_each_word_of_str(original_title[17:])
                adapter['original_title'] = original_title

            # ERASE STR AND CONVERT TO INT
            elif field_name == 'year':
                year = adapter.get('year')
                if year is not None:
                    year = int(''.join(filter(str.isdigit, year)))
                adapter['year'] = year
                
            # NOTHING DONE
            elif field_name == 'public':
                public = adapter.get('public')
                if public is not None:
                    adapter['public'] = public

            # SPLIT VALUE INTO 2 : HOURS & MINUTES
            # CONVERT IN MINUTES
            elif field_name == "screening":
                screening = adapter.get('screening')
                if screening is not None:
                    screen_split = screening.split(' ')
                    if len(screen_split) > 1:
                        if "h" in screening:
                            hours = ''.join(filter(str.isdigit, screen_split[0]))
                            minutes = ''.join(filter(str.isdigit, screen_split[1]))
                            time = int(hours) * 60 + int(minutes)
                        else:
                            time = ''.join(filter(str.isdigit, screen_split[0]))
                    adapter['screening'] = time

            # CONVERT TO FLOAT
            elif field_name == "mark":
                mark = adapter.get('mark')
                if mark is not None:
                    adapter['mark'] = float(mark)
                    
            # CONVERT "M" & "K" TO REAL NUMBERS
            # CONVERT RESULT TO INT    
            elif field_name == "marks_nb":
                marks_nb = adapter.get('marks_nb')
                if marks_nb is not None:
                    if "M" in marks_nb:
                        marks_nb = float(''.join(filter(str.isdigit, marks_nb)))
                        marks_nb = marks_nb * 1000000
                    elif "K" in marks_nb:
                        marks_nb = float(''.join(filter(str.isdigit, marks_nb)))
                        marks_nb = marks_nb * 1000
                adapter['marks_nb'] = int(marks_nb)

            # KEEP ONLY DIGITS AND CONVERT TO INT
            elif field_name == "boxoffice":
                boxoffice = adapter.get('boxoffice')
                if boxoffice is not None:
                    boxoffice = ''.join(filter(str.isdigit, boxoffice))
                adapter['boxoffice'] = int(boxoffice)

            # KEEP ONLY DIGITS AND CONVERT TO US DOLLAR ($) FLOATS
            elif field_name == "budget":
                budget = adapter.get('budget')
                if budget is not None:
                    if budget[0] == '¥':
                        budget = int(int(''.join(filter(str.isdigit, budget))) / 150)
                    elif budget[0] == '₩':
                        budget = int(int(''.join(filter(str.isdigit, budget))) * 0.00075)
                    elif budget[0] == '€':
                        budget = int(int(''.join(filter(str.isdigit, budget))) * 1.09)
                    elif budget[0] == 'A':
                        budget = int(int(''.join(filter(str.isdigit, budget))) * 0.65)
                    elif budget[0] == '£':
                        budget = int(int(''.join(filter(str.isdigit, budget))) * 1.27)
                    elif budget[0] == '₹':
                        budget = int(int(''.join(filter(str.isdigit, budget))) * 0.012)
                    elif budget[:2] == 'DE':
                        budget = int(int(''.join(filter(str.isdigit, budget))) * 0.55494846)
                    elif budget[:2] == 'DK':
                        budget = int(int(''.join(filter(str.isdigit, budget))) * 0.15)
                    elif budget[0] == "F":
                        budget = int(int(''.join(filter(str.isdigit, budget))) * 0.16)
                    elif budget[0] == "R":
                        budget = int(int(''.join(filter(str.isdigit, budget))) * 0.2)
                    else:
                        budget = int(int(''.join(filter(str.isdigit, budget))))
                adapter['budget'] = budget

            # ERASE USELESS STR CHARACTERS & CONVERT TO STRINGS EACH SEPARATED BY A COMMA
            elif field_name == "category":
                category = adapter.get('category')
                if category is not None:
                    category = ', '.join(category)
                    category = category.replace("'", "").replace("[", "").replace("]", "")
                adapter['category'] = str(category)
            
            
            # ERASE USELESS STR CHARACTERS & CONVERT TO STRINGS EACH SEPARATED BY A COMMA
            elif field_name == "country":
                country = adapter.get('country')
                if country is not None:
                    country = ', '.join(country)
                    country = country.replace("'", "").replace("[", "").replace("]", "")
                adapter['country'] = str(country)

            # ERASE USELESS STR CHARACTERS & CONVERT TO STRINGS EACH SEPARATED BY A COMMA
            elif field_name == "casting":
                casting = adapter.get('casting')
                if casting is not None:
                    casting = set(casting)
                    casting = ', '.join(casting)
                    casting = casting.replace("'", "").replace("[", "").replace("]", "")
                adapter['casting'] = casting
                
        # ADD CLEANED DATA TO TABLE
        self.cur.execute("""
                         INSERT INTO movies (url, title, original_title, year, public, screening, mark, marks_nb, category, synopsis, director, budget, boxoffice, country, casting, poster) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                            adapter.get('poster'),
                         ))

        # execute insert of data into the database
        self.con.commit()

        return item
    

class CountriesPipeline:
    def __init__(self) -> None:
        self.con = sqlite3.connect('../data/database/imdb.db')
        self.cur = self.con.cursor()
        self.cur.execute("""
                        CREATE TABLE IF NOT EXISTS countries(
                         country TEXT UNIQUE
                        )""")
        

    def process_item(self, item, spider):
        countries = {
            'France',
            'United States',
            'Poland',
            'Germany',
            'United Kingdom',
            'South Korea',
            'Italy',
            'Brazil',
            'Germany',
            'Japan',
            'India',
            'Mexico',
            'Algeria',
            'Turkey',
            'Malta',
            'Morocco',
            'South Africa',
            'Ireland',
            'Sweden',
            'Canada',
            'Australia',
            'Argentina',
            'Spain',
            'Austria',
            'Iran',
            'China',
            'West Germany',
            'Denmark',
            'Soviet Union',
            'Norway',
            'Hong Kong',
            'New Zealand',
            'United Arab Emirates',
            'Hungary',
            'Jordan',
            'Gambia',
            'Lebanon',
            'Cyprus',
            'Qatar',
            'Czech Republic'
        }

        for country in countries:
            self.cur.execute("""
                            INSERT OR IGNORE INTO countries (country) VALUES (?)
                             """,
                             (country,))
            
        self.con.commit()
        return item

    
class CategoriesPipeline:
    def __init__(self) -> None:
        self.con = sqlite3.connect('../data/database/imdb.db')
        self.cur = self.con.cursor()
        self.cur.execute("""
                         CREATE TABLE IF NOT EXISTS categories(
                             category TEXT UNIQUE
                             )""")
    
    def process_item(self, item, spider):
        categories = {
            'Action',
            'Comedy',
            'Romance',
            'Adventure',
            'Crime', 
            'Thriller',
            'Drama',
            'Fantasy',
            'Sci-Fi',
            'Animation',
            'Family',
            'War',
            'Musical',
            'Music',
            'Film-Noir',
            'Sport',
            'Mystery',
            'Biography',
            'Horror',
            'Western',
            'History'
        }
        
        for category in categories:
            self.cur.execute("""
                            INSERT OR IGNORE INTO categories (category) VALUES (?)
                            """,
                            (category,))
            
        self.con.commit()
        return item


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
                             nb_seasons INTEGER,
                             nb_episodes INTEGER,
                             episode_length INTEGER,
                             synopsis TEXT,
                             mark REAL,
                             marks_nb INTEGER
                         )
                         """)
        
    
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        field_names = adapter.field_names()
        for field_name in field_names:

            if field_name == "nb_seasons":
                years = adapter.get('years')
                nb_seasons = adapter.get('nb_seasons')
                if '–' not in years and nb_seasons is None:
                    nb_seasons = 1
                adapter['nb_seasons'] = nb_seasons

            elif field_name == 'nb_seasons':
                nb_seasons = adapter.get('nb_seasons')
                if nb_seasons is not None:
                    nb_seasons = int(nb_seasons)
                adapter['nb_seasons'] = nb_seasons

            elif field_name == "nb_episodes":
                nb_episodes = adapter.get('nb_episodes')
                if nb_episodes is not None:
                    nb_episodes = int(nb_episodes)
                adapter['nb_episodes'] = nb_episodes

            elif field_name == "episode_length":
                episode_length = adapter.get('episode_length')
                nb_episodes = adapter.get('nb_episodes')
                if episode_length is not None:
                    if 'h' in episode_length:
                        split = episode_length.split(' ')
                        hours = ''.join(filter(str.isdigit, split[0]))
                        minutes = ''.join(filter(str.isdigit, split[1]))
                        episode_length = round((((int(hours) * 60) + int(minutes)) / int(nb_episodes)), 2)
                    else:
                        episode_length = ''.join(filter(str.isdigit, episode_length))
                adapter["episode_length"] = episode_length
            
            elif field_name == "mark":
                mark = adapter.get('mark')
                if mark is not None:
                    mark = float(mark)
                adapter['mark'] = mark

            elif field_name == "marks_nb":
                marks_nb = adapter.get('marks_nb')
                if marks_nb is not None:
                    if 'M' in marks_nb:
                        marks_nb = float(''.join(filter(str.isdigit, marks_nb)))
                        marks_nb *= 1000000
                    elif "K" in marks_nb:
                        marks_nb = float(''.join(filter(str.isdigit, marks_nb)))
                        marks_nb *= 1000
                adapter['marks_nb'] = marks_nb

        # ADD CLEANED DATA TO TABLE
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
    
    # close cursor & connection to db
    def close_spider(self, spider): 
        self.cur.close()
        self.con.close()