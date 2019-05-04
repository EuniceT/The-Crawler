import logging
import re
import os
import lxml.html
import re
from urllib.parse import urlparse
from corpus import Corpus
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier):
        self.frontier = frontier
        self.corpus = Corpus()
        self.url_dict = {}

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.fetch_url(url)

            for next_link in self.extract_next_links(url_data):
                if self.corpus.get_file_name(next_link) is not None:
                    if self.is_valid(next_link):
                        self.frontier.add_url(next_link)

    def fetch_url(self, url):
        """
        This method, using the given url, should find the corresponding file in the corpus and return a dictionary
        containing the url, content of the file in binary format and the content size in bytes
        :param url: the url to be fetched
        :return: a dictionary containing the url, content and the size of the content. If the url does not
        exist in the corpus, a dictionary with content set to None and size set to 0 can be returned.
        """
        url_data = {
            "url": url,
            "content": None,
            "size": 0
        }

        file_name = self.corpus.get_file_name(url)

        if file_name != None:
            with open(file_name, "rb") as text_file:
                html_data = text_file.read()
                url_data["content"] = html_data

            url_data["size"] = os.path.getsize(file_name)

        return url_data

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.
        Suggested library: lxml
        """

        html = url_data["content"]
        url = url_data["url"]

        html = lxml.html.make_links_absolute(html, url)

        outputLinks = []

        for ele, attr, link, pos in lxml.html.iterlinks(html):
            if attr == "href":
                outputLinks.append(link)
        
        return outputLinks

    def dup_subdomain(self, url_path):
        p_list = url_path.split("/")
        p_set = set(p_list)
        return len(p_set) != len(p_list)

    def not_similar_links(self, parsed):
        path = parsed.geturl()
        p_list = path.split("?")

        if len(p_list) > 1:
            query = p_list[1]
            e_query = re.sub(r'(\w+=)(\w+)', r"\1", query)
            if p_list[0] not in self.url_dict:
                self.url_dict[p_list[0]] = e_query
            else:            
                ratio = SequenceMatcher(None,self.url_dict[p_list[0]], e_query).ratio()
                return ratio < 0.5
        else:
            return True


    '''1:10k  2:9219'''
    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        try:
            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower()) \
                    and not self.dup_subdomain(parsed.path.lower()) \
                    and self.not_similar_links(parsed) \
                    and len(parsed.path.lower()) < 40 

        except TypeError:
            print("TypeError for ", parsed)
            return False

    
