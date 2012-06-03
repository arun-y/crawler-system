#!/usr/bin/python
#####################+#+#-+######################
#   This module contains class definition 
#   for main crawler application
#############################################
import json, gzip, StringIO, time, urlparse, urllib2
import os
import threading
from xml.dom.minidom import parse, parseString
from robots_extractor import RobotsTxtFetcher
from httplib2.socks import HTTPError

ROOT_FOLDER = "/data/crawler_system/crawled_hosts"
FRONTIER_PATH = "/data/crawler_system/frontier"
USER_AGENT = "CanopusBot/0.1 (Ubuntu 11.10; Linux x86_64)"
EMAIL = "arunwizz@gmail.com"
       
class RobotsTxtFetcherThread(threading.Thread):
    
    def __init__(self, host_name):
        threading.Thread.__init__(self)
        self.__host_name = host_name
        
    def __urlopen(self, __url):
        try:
            req = urllib2.Request(url=__url)
            req.add_header('User-Agent', USER_AGENT)
            req.add_header("From", EMAIL)
            return urllib2.urlopen(req)
        except HTTPError as (errno, strerror):
            print "HTTP error({0}): {1}".format(errno, strerror)        
        
    def sitemap_index_parser(self, sitemap_index_url):
        parse_result = urlparse.urlparse(sitemap_index_url)
        host = parse_result[1]
        path = parse_result[2].replace('/', '_')
        print "parsing sitemap {sitemap} from host {host}".format(sitemap=path, host=host)
        sitemap_index_fd = self.__urlopen(sitemap_index_url)
        if sitemap_index_fd.getcode() == 200:
            if sitemap_index_fd.info()['Content-type'] in 'application/gzip, application/x-gzip, application/x-gunzip, application/gzipped, application/gzip-compressed, application/x-compressed, application/x-compress, gzip/document, application/octet-stream':
                #then gunzip
                #store the content into sitemap_index_xml
                sitemap_index_stringIO = StringIO.StringIO(sitemap_index_fd.read())
                sitemap_index_gunziped = gzip.GzipFile(fileobj=sitemap_index_stringIO, mode='r')
                sitemap_index_dom = parseString(sitemap_index_gunziped.read())
                pass
            else:# assuming it's xml
                sitemap_index_dom = parse(sitemap_index_fd)
            root_name = sitemap_index_dom.documentElement.nodeName
            if root_name == 'sitemapindex':
                for sitemap_loc in sitemap_index_dom.getElementsByTagName('loc'):
                    sitemap_index_url = sitemap_loc.toxml().strip()[5:-6]#take out <loc/>
                    #give some rest to host
                    print "giving rest to host: {0}".format(host) 
                    time.sleep(5)
                    self.sitemap_index_parser(sitemap_index_url)
                    pass
                pass
            else:# 'urlset'
                frontier_request_file_path = FRONTIER_PATH + "/" + host + path + ".request" 
                frontier_request_file = open(frontier_request_file_path, 'w')
                print "Writing urlset to " + frontier_request_file_path
                for url_loc in sitemap_index_dom.getElementsByTagName('loc'):
                    url_loc_url = url_loc.toxml().strip()[5:-6]
                    #write above url into frontier
                    frontier_request_file.write(url_loc_url + '\n')
                frontier_request_file.close()
                #indicate file writing completion
                open(frontier_request_file_path + '.ready', 'w').close()
                pass
        else:
            print "Error getting sitemap index " + sitemap_index_url        
        
    def run(self):
        #print threading.current_thread().getName()
        print "Fetching " + self.__host_name 
        rf = RobotsTxtFetcher()
        robots_map = rf.fetch_robots_txt_map(self.__host_name)
        robots_txt_json = json.dumps(robots_map, indent=4)
        #save the robots.json object at the root of host folder
        host_root_folder = ROOT_FOLDER + "/" + self.__host_name
        if not os.path.exists(host_root_folder):
            os.makedirs(host_root_folder)
        robots_json_file = open(host_root_folder + "/robots.json", 'w')
        robots_json_file.write(robots_txt_json)
        robots_json_file.close()
        #process sitemap_url list and push the extracted urls into frontier
        for sitemap_url in robots_map['sm']:
            self.sitemap_index_parser(sitemap_url)

class Crawler:
    
    def __init__(self, root_path):
        self._root_path = root_path
        
    def crawl(self):
        #look for seed.file
        try:
            seed_file = open (self._root_path + '/seed.file')
            #loop over every line, ignore commented out hosts
            fetcherThreadList = []
            for host_name in seed_file.readlines():
                host_name = host_name[:-1]#drop the last '\n' 
                if host_name[0] != '#':
                    #spawn threads for each host
                    #print "Processing ", host_name
                    t = RobotsTxtFetcherThread(host_name)
                    t.setDaemon(True)
                    t.start()
                    fetcherThreadList.append(t)
                    pass
                else:
                    #ignore this host
                    print "Ignoring host ", host_name[1:]
            #wait till all fetcher thread is over
            for fetcherThread in fetcherThreadList:
                fetcherThread.join() 
        except IOError as (errno, strerror):
            print "I/O error({0}): {1}".format(errno, strerror)
    
if __name__ == '__main__':
    import sys
    
    print 'Instantiating Crawler'
    c = Crawler(sys.argv[1])
    c.crawl()
    
