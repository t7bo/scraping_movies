import scrapy
from moviescraper.items import MovieItem
from loguru import logger 


class MoviespiderSpider(scrapy.Spider):
    name = "moviespider"
    allowed_domains = ["www.imdb.com"]
    start_urls = ["https://www.imdb.com/chart/top"]
    custom_settings = {
        'ITEM_PIPELINES': {"moviescraper.pipelines.MoviescraperPipeline": 100,
                           "moviescraper.pipelines.CategoriesPipeline": 200,
                           "moviescraper.pipelines.CountriesPipeline": 300}
    }

    def parse(self, response):
        movies = response.css("li.ipc-metadata-list-summary-item")
        for movie in movies:
            relative_url = movie.css('div.ipc-title a.ipc-title-link-wrapper::attr(href)').get()
            movie_url = 'https://www.imdb.com' + relative_url
            yield response.follow(movie_url, callback=self.parsemoviepage) # callback = quelle fonction il va executer ensuite

    @logger.catch
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
        movie_item['category'] = response.xpath('//section/div[3]/div[2]/div[1]/section/div[1]/div[2]/a/span/text()').getall()
        movie_item['synopsis'] = response.xpath('//span[@data-testid="plot-xl"]/text()').get()
        movie_item['director'] = response.xpath('//section/div[3]/div[2]/div[1]/section/div[2]/div/ul/li[1]/div/ul/li/a/text()').get()
        movie_item['budget'] = response.xpath('//li[@data-testid="title-boxoffice-budget"]/div/ul/li[@role="presentation"]/span/text()').get()
        movie_item['boxoffice'] = response.xpath('//li[@data-testid="title-boxoffice-cumulativeworldwidegross"]/div/ul/li/span/text()').get()
        movie_item['country'] = response.xpath('//section[@data-testid="Details"]/div[2]/ul/li[2]/div/ul/li/a/text()').getall()
        movie_item['casting'] = response.xpath('//div[@data-testid="shoveler-items-container"]//div[@data-testid="title-cast-item"]//div[2]/a/text()').getall()
        
        poster_url = response.css('a.ipc-lockup-overlay::attr(href)').get()
        if poster_url:
            yield response.follow('https://www.imdb.com' + poster_url, callback=self.parse_poster_page, meta={'movie_item': movie_item})
        else:
            yield movie_item
            
    @logger.catch
    def parse_poster_page(self, response):
        movie_item = response.meta['movie_item']
        movie_item['poster'] = response.xpath('//img[@class="sc-7c0a9e7c-0 eWmrns"]/@src').get()
        yield movie_item
