import scrapy
from moviescraper.items import MovieItem
import json

class MoviespiderSpider(scrapy.Spider):
    name = "moviespider"
    allowed_domains = ["www.imdb.com"]
    start_urls = ["https://www.imdb.com/chart/top"]
    custom_settings = {
        'ITEM_PIPELINES': {"moviescraper.pipelines.MoviescraperPipeline": 300},
    }

    # HEADERS = {
    #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    #     'Accept-Language' : 'en-US,en;q=0.9'
    #     }

    def parse(self, response):
        # Première étape : sélectionner chaque block de film d'une page
        # le mieux serait de sélectionner chaque <a ipc-title-link-wrapper> dirigeant vers la page du film
        # movies = response.css("div.ipc-title")
        movies = response.css("li.ipc-metadata-list-summary-item")
        for movie in movies:
            relative_url = movie.css('div.ipc-title a.ipc-title-link-wrapper::attr(href)').get()
            movie_url = 'https://www.imdb.com' + relative_url
            yield response.follow(movie_url, callback=self.parsemoviepage)

    def parsemoviepage(self, response):
        movie_item = MovieItem()
        movie_item["url"] = response.url
        movie_item['title'] = response.xpath('//h1[@data-testid="hero__pageTitle"]//span/text()').get()
        movie_item['original_title'] = response.xpath('//div[@class="sc-d8941411-1 fTeJrK"]/text()').get()
        movie_item['year'] = response.xpath('//section/div[2]/div[1]/ul/li[1]/a/text()').get()
        movie_item['public'] = response.xpath('//section/div[2]/div[1]/ul/li[2]/a/text()').get()
        movie_item['screening'] = response.xpath('//section/div[2]/div[1]/ul/li[3]/text()').get()
        movie_item['mark'] = response.xpath('//section/div[2]/div[2]/div/div[1]/a/span/div/div[2]/div[1]/span/text()').get()
        movie_item['marks_nb'] = response.xpath('//section/div[2]/div[2]/div/div[1]/a/span/div/div[2]/div[3]/text()').get()
        movie_item['popularity'] = response.xpath('//div[@data-testid="hero-rating-bar__popularity__score"]/text()').get()
        movie_item['category'] = json.dumps(response.xpath('//section/div[3]/div[2]/div[1]/section/div[1]/div[2]/a/span/text()').getall())
        movie_item['synopsis'] = response.xpath('//span[@data-testid="plot-xl"]/text()').get()
        movie_item['director'] = response.xpath('//section/div[3]/div[2]/div[1]/section/div[2]/div/ul/li[1]/div/ul/li/a/text()').get()
        movie_item['budget'] = response.xpath('//li[@data-testid="title-boxoffice-budget"]/div/ul/li[@role="presentation"]/span/text()').get()
        movie_item['boxoffice'] = response.xpath('//li[@data-testid="title-boxoffice-cumulativeworldwidegross"]/div/ul/li/span/text()').get()
        movie_item['country'] = json.dumps(response.xpath('//section[@data-testid="Details"]/div[2]/ul/li[2]/div/ul/li/a/text()').getall())
        movie_item['casting'] = json.dumps(response.xpath('//div[@role="presentation"]/ul[@role="presentation"]/li[3]/div[@class="ipc-metadata-list-item__content-container"]/ul[@role="presentation"]/li[@role="presentation"]/a/text()').getall())
        yield movie_item
