import atexit
import logging
import sys

from crawler import Crawler
from frontier import Frontier

if __name__ == "__main__":
    # Configures basic logging
    logging.basicConfig(filename = "out.txt", format='%(asctime)s (%(name)s) %(levelname)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.INFO)

    # Instantiates frontier and loads the last state if exists
    frontier = Frontier()   
    frontier.load_frontier()
    # Registers a shutdown hook to save frontier state upon unexpected shutdown
    atexit.register(frontier.save_frontier)

    # Instantiates a crawler object and starts crawling
    crawler = Crawler(frontier)
    crawler.start_crawling()

    
    sys.stdout = open("url_out.txt", "w")

    print("DOWNLOADED URLS: ", crawler.downloaded_urls, "\n\n")
    print("TRAPS: ", crawler.traps, "\n\n")
    print("SUBDOMAINS: ", crawler.subdomains, "\n\n")

    # url_data = crawler.fetch_url("https://ics.uci.edu")
    # crawler.extract_next_links(url_data)
