import logging
import re
import os
from difflib import SequenceMatcher
from urllib.parse import urlparse
from corpus import Corpus
import lxml.html
from lxml import etree
from collections import deque

logger = logging.getLogger(__name__)


class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier):
        self.frontier = frontier
        self.corpus = Corpus()
        self.vis_sub = dict()
        self.out_links = dict()
        self.checked = dict()
        self.traps=set()
        # self.q = dict()
        self.url_queue = deque([])

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
        local_file = self.corpus.get_file_name(url)
        url_data["url"] =url
        sz = os.path.getsize(local_file)
        if local_file!= None:
            file = open(local_file,"rb").read()
            url_data["content"] = file
            url_data["size"] = sz
        else:
            url_data["content"] = None
            url_data["size"] = 0
        #size is the content in bytes
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
        outputLinks = []
        #reading the binary and parsing it
        # xread = url_data["content"] 
        # src = lxml.html.fromstring(xread)
        # src.make_links_absolute(url_data["url"])
        # links = src.xpath('//a/@href')
        # for l in links:
        #     if type(l)==list:
        #          outputLinks.extend(l)
        #     else:
        #          outputLinks.append(l)  

        # #add url to dict to see outlinks 
        self.out_links[url_data["url"]] = len(outputLinks) 
        html = url_data["content"]
        url = url_data["url"]
        html = lxml.html.make_links_absolute(html,url)
        for ele, attr, link, pos in lxml.html.iterlinks(html):
            if attr == "href":
                outputLinks.append(link)
        return outputLinks

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        host = parsed.hostname
        #checking for subdomain
        #add to ics.uci.edu(treat www as the same)
        part = parsed.path.lower()
        pat = parsed.path.split('/')
        if parsed.scheme not in set(["http", "https"]):
            return False
        #check repeats
        #if it repeats, then it is a trap
        if len(part)>1: 
            if len(pat)!=len(set(pat)):
                self.traps.add(url)
                return False
        #make a threshold
        #like 5000
        # threshold= 230

        #like 5200
        #threshold = 275

        #like 5300
        # threshold = 300

        #like 5400
        # threshold = 325

        #like 5600
        threshold = 500
        query = parsed.query

        #string of everything before the query
        url_path = url[:url.find(query)-1] 

        # self.q[query] = url_path
        # for k,v in self.q.items():
        #   if len()

        # seq = difflib.SequenceMatcher(None, )

        #if it has a query
        #check if query is in if it is add

        if len(query)!=0:
          if url_path in self.checked:
              self.checked[url_path]+=1
          else:
              self.checked[url_path] =1
          if self.checked[url_path]>threshold:
              self.traps.add(url)
              return False

        #check if dict is bigger than threshold

        # p_list = url.split("?")

        # self.url_queue.append(url)
        # num_similar = 0
        # url_path = url[:url.find(query)-1] 
        # if "?" in url:
        #     # e_path = re.sub(r'(\w+=)(\w+)', r"\1", url)
        #     for item in self.url_queue:
        #         # prev = re.sub(r'(\w+=)(\w+)', r"\1", item)
        #         ratio = SequenceMatcher(None, item, url_path).ratio()
        #         if ratio > 0.5:
        #             num_similar+=1
            
        # if len(self.url_queue) > 4:
        #     self.url_queue.popleft()
        # else:
        #     if num_similar > 2:
        #         return False
        try:
            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

        except TypeError:
            print("TypeError for ", parsed)
            return False
        finally:
            #check if url is subdomain and add
            #if its ics.uci.edu force it to be the same as www.ics.uci.edu
            #check if url is subdomain and add
            #if its ics.uci.edu force it to be the same as www.ics.uci.edu
            #also remove from subdomain if query dict is bigger than threshold
            if "www.ics.uci.edu" in host:
                subdomain = host
            elif host.split('.')[0]=='ics':
                    subdomain = 'www.'+ host
            else:
                subdomain = host.split('.')[0]+'.ics.uci.edu'
            if subdomain in self.vis_sub:
                self.vis_sub[subdomain]+=1 
            else:
                self.vis_sub[subdomain] =1



