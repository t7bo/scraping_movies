# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class MoviescraperItem(scrapy.Item):
    # define the fields for your item here like:
    name = scrapy.Field()
    pass

class MovieItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    original_title = scrapy.Field()
    year = scrapy.Field()
    public = scrapy.Field()
    screening = scrapy.Field()
    mark = scrapy.Field()
    marks_nb = scrapy.Field()
    category = scrapy.Field()
    synopsis = scrapy.Field()
    director = scrapy.Field()
    budget = scrapy.Field()
    boxoffice = scrapy.Field()
    country = scrapy.Field()
    casting = scrapy.Field()
    poster = scrapy.Field()
    
class SerieItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    years = scrapy.Field()
    nb_seasons = scrapy.Field()
    nb_episodes = scrapy.Field()
    episode_length = scrapy.Field()
    synopsis = scrapy.Field()
    mark = scrapy.Field()
    marks_nb = scrapy.Field()