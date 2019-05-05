import logging
import re
import os
import lxml.html
import re
from urllib.parse import urlparse
from corpus import Corpus
from lxml.html import fromstring
from difflib import SequenceMatcher
from collections import deque
import sys
import operator

logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier):
        self.frontier = frontier
        self.corpus = Corpus()
        
        self.subdomains = {}
        self.subdomains["ics.uci.edu"] = 0
        self.downloaded_urls = []
        self.out_links = {}
        self.traps = []
        
        self.checked = {}
        #self.url_failed = []


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

        self.out_links[url] = len(outputLinks)
        return outputLinks


    """
    Gets the subdomain from the hostname and takse out the www. For example, if www.calendar.ics.uci.edu was the hostname
    , then it would return calender. Returns "None" if ics.uci.edu is not found.
    """
    def get_subdomain(self, hostname):
        start = hostname.find("www")
        len_start = 4
        if start == -1:
            start = hostname.find("//")
            len_start = 1
        
        end = hostname.find(".ics.uci.edu")
        if end != -1:
            return hostname[start+len_start:end+len(".ics.uci.edu")]
        return "None"


    """
    Adds suddomain to a dictionary and tracks how many times it has been visited.
    """
    def add_subdomain(self, parsed):
        subd = self.get_subdomain(parsed.hostname)
        if subd != "None":
            if subd in self.subdomains:
                self.subdomains[subd] += 1

                if "ics.uci.edu" != subd and "ics.uci.edu" in self.subdomains:
                        self.subdomains["ics.uci.edu"] += 1
            else:
                self.subdomains[subd] = 1


    """
    Returns the path with the most outlinks.
    """
    def get_max_out_links(self):
        return max(self.out_links, key=lambda x: self.out_links[x])


    """
    Returns true if the subdomain has duplicate folders, such as www.ics.uci.edu/datasets/datasets/datasets/datasets.
    False otherwise.
    """
    def dup_subdomain(self, url_path):
        p_list = list(filter(None, url_path.split("/")))
        p_set = set(p_list)
        #return len(p_set) != len(p_list)
        return len(p_set) + 3 < len(p_list) # Threshold of 3


    """
    Adds the url to a dictionary with all the previously checked urls that contain a query. Returns false if the
    url has been visited more times than the threshold. True otherwise.
    th = 500 -> 5100, th = 750 -> 5200
    th = 500 -> 5200 with threshold +3 <, +5 <
    th = 1000 -> 5500
    """
    def pass_threshold(self, url, query):    
        threshold = 1000

        #string of everything before the query
        url_path = url[:url.find(query)-1]

        if len(query) != 0:
          if url_path in self.checked:
              self.checked[url_path] += 1
          else:
              self.checked[url_path] = 1

          if self.checked[url_path] > threshold:
              return True

        return False


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
            if self.dup_subdomain(parsed.path.lower()) or self.pass_threshold(parsed.path.lower(), parsed.query):
                if url not in self.traps:
                    self.traps.append(url)
                return False

            if ".ics.uci.edu" in parsed.hostname:

                if not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                + "|thmx|mso|arff|rtf|jar|csv" \
                                + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower()):
                
                    if url not in self.downloaded_urls:
                        self.downloaded_urls.append(url)

                    return True
            return False

        except TypeError:
            print("TypeError for ", parsed)
            return False

        finally:
            self.add_subdomain(parsed)
