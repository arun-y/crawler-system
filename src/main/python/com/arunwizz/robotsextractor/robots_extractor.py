#!/usr/bin/python
'''
Created on 20-May-2012

@author: aruny
'''

import urllib2, json
from httplib2.socks import HTTPError

USER_AGENT = "CanopusBot/0.1 (Ubuntu 11.10; Linux x86_64)"
EMAIL = "arunwizz@gmail.com"

class RobotsTxtFetcher:
    """This class fetches the robots.txt for a given host, if available"""
    
    def __urlopen(self, __url):
        try:
            req = urllib2.Request(url=__url)
            req.add_header('User-Agent', USER_AGENT)
            req.add_header("From", EMAIL)
            return urllib2.urlopen(req)
        except HTTPError as (errno, strerror):
            print "HTTP error({0}): {1}".format(errno, strerror)
    
    def fetch_robots_txt_map(self, host_name):
        """return robots.txt content as dictionary object"""
        
        robots_map = {}
        robots_txt_url = "http://" + host_name + "/robots.txt"
        f = self.__urlopen(robots_txt_url)
        if f.getcode() == 200:
            robots_map['sm'] = [] #site-map list
            current_user_agent = ''
            for line in f.readlines():
                line = line.lower().strip()
		if line[0] == '#':
			continue
                split = line.partition(':')
                split_key = split[0].strip()
                split_value = split[2].strip()
                if split_key == line: #line doesn't contain ':', ignore
                    pass
                elif split_key == "User-agent".lower():
                    current_user_agent = split_value 
                    robots_map[current_user_agent] = {'da':[], 'a':[], 'cd':[]}
                    #dictionary{disallow, allow, crawl_delay}
                elif split_key == "Disallow".lower() and len(split_value) != 0:
                    robots_map[current_user_agent]['da'].append(split_value)
                elif split_key == "Allow".lower() and len(split_value) != 0:
                    robots_map[current_user_agent]['a'].append(split_value)
                elif split_key == "Crawl-delay".lower() and len(split_value) != 0:
                    robots_map[current_user_agent]['cd'].append(split_value)
                elif split_key == "Sitemap".lower() and len(split_value) != 0:
                    if split_key[0] != "#":
                        robots_map['sm'].append(split_value)
        else:
            print f.getcode(), " error getting robots.txt found for host: ", self.__host_name        

        return robots_map
 
    def fetch_robots_txt_json(self, host_name):
        robots_map = self.fetch_robots_txt_map(host_name)
        robots_txt_json = json.dumps(robots_map, indent=4)
        return robots_txt_json
        
        
if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print "Expecting just one host name as an argument"
    else:
        print 'Fetching robots.txt for ' + sys.argv[1]
        rf = RobotsTxtFetcher()
        print rf.fetch_robots_txt_json(sys.argv[1])
    
 
