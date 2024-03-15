import scrapy


class SeriespiderSpider(scrapy.Spider):
    name = "seriespider"
    allowed_domains = ["www.imdb.com"]
    start_urls = ["https://www.imdb.com/chart/toptv/"]

    def parse(self, response):
        series = response.css("li.ipc-metadata-list-summary-item")
        for serie in series:
            relative_url = serie.css('div.ipc-poster--base a::attr(href)').get()
            serie_url = 'https://www.imdb.com' + relative_url
            yield response.follow(serie_url, callback=self.parseseriepage)

    def parseseriepage(self, response):
        yield {
            'url' : response.url, 
            'title' : response.xpath('//main/div/section[1]/section/div[3]/section/section/div[2]/div[1]/h1/span/text()').get(),
            'years' : response.xpath('//main/div/section[1]/section/div[3]/section/section/div[2]/div[1]/ul/li[2]/a/text()').get(),
            'nb_seasons' : response.xpath('//*[@id="browse-episodes-season"]/option[2]/text()').get(),
            'nb_episodes' : response.xpath('//main/div/section[1]/div/section/div/div[1]/section[2]/div[1]/a/h3/span[2]/text()').get(),
            'episode_length' : response.xpath('//main/div/section[1]/section/div[3]/section/section/div[2]/div[1]/ul/li[4]/text()').get(),
            'synopsis' : response.xpath('//main/div/section[1]/section/div[3]/section/section/div[3]/div[2]/div[1]/section/p/span[1]/text()').get(),
            'mark' : response.xpath('//main/div/section[1]/section/div[3]/section/section/div[3]/div[2]/div[2]/div[1]/div/div[1]/a/span/div/div[2]/div[1]/span[1]/text()').get(),
            'marks_nb' : response.xpath('//main/div/section[1]/section/div[3]/section/section/div[3]/div[2]/div[2]/div[1]/div/div[1]/a/span/div/div[2]/div[3]/text()').get(),
        }