import scrapy


class SeriespiderSpider(scrapy.Spider):
    name = "seriespider"
    allowed_domains = ["www.imdb.com"]
    start_urls = ["https://www.imdb.com/chart/toptv/"]

    def parse(self, response):
        pass
