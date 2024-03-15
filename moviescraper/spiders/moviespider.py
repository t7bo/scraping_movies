import scrapy

class MoviespiderSpider(scrapy.Spider):
    name = "moviespider"
    allowed_domains = ["www.imdb.com"]
    start_urls = ["https://www.imdb.com/chart/top/?ref_=nv_mv_250"]

    def parse(self, response):
        # Première étape : sélectionner chaque block de livre d'une page
        # le mieux serait de sélectionner chaque <a ipc-title-link-wrapper> dirigeant vers la page du film
        # movies = response.css("div.ipc-title")
        movies = response.css("li.ipc-metadata-list-summary-item")
        for movie in movies:
            relative_url = movie.css('div.ipc-title a.ipc-title-link-wrapper::attr(href)').get()
            movie_url = 'https://www.imdb.com' + relative_url
            yield response.follow(movie_url, callback=self.parsemoviepage)

    def parsemoviepage(self, response):
        yield {
            'url' : response.url,
            'title' : response.xpath('//h1[@data-testid="hero__pageTitle"]//span/text()').get(),
            'original_title' : response.xpath('//div[@class="sc-d8941411-1 fTeJrK"]/text()').get(),
            'year' : response.xpath('//section/div[2]/div[1]/ul/li[1]/a/text()').get(),
            'public' : response.xpath('//section/div[2]/div[1]/ul/li[2]/a/text()').get(),
            'screening' : response.xpath('//section/div[2]/div[1]/ul/li[3]/text()').get(),
        }