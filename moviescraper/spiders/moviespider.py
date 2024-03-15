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
            yield {
                'url' : movie.css('div.ipc-title a.ipc-title-link-wrapper::attr(href)').get()
            }
