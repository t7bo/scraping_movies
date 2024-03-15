import scrapy
from moviescraper.items import MovieItem

class MoviespiderSpider(scrapy.Spider):
    name = "moviespider"
    allowed_domains = ["www.imdb.com"]
    start_urls = ["https://www.imdb.com/chart/top"]
    custom_settings = {
        'ITEM_PIPELINES': {"moviescraper.pipelines.MoviescraperPipeline": 300},
    }

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
        movie_item['category'] = response.xpath('//section/div[3]/div[2]/div[1]/section/div[1]/div[2]/a/span/text()').get()
        movie_item['synopsis'] = response.xpath('//span[@data-testid="plot-xl"]/text()').get()
        movie_item['director'] = response.xpath('//section/div[3]/div[2]/div[1]/section/div[2]/div/ul/li[1]/div/ul/li/a/text()').get()
            # 'casting' : list(response.xpath('//section/div[3]/div[2]/div[1]/section/div[2]/div/ul/li[3]/div/ul//following-sibling::li[0]//a/text()').get(),
            #                  response.xpath('//section/div[3]/div[2]/div[1]/section/div[2]/div/ul/li[3]/div/ul//following-sibling::li[1]//a/text()').get(),
            #                  response.xpath('//section/div[3]/div[2]/div[1]/section/div[3]/div/ul/li[3]/div/ul//following-sibling::li[2]//a/text()').get()),
        movie_item['budget'] = response.xpath('//main/div/section[1]/div/section/div/div[1]/section[12]/div[2]/ul/li[1]/div/ul/li/span/text()').get()
        movie_item['boxoffice'] = response.xpath('//main/div/section[1]/div/section/div/div[1]/section[12]/div[2]/ul/li[4]/div/ul/li/span/text()').get()
        yield movie_item
