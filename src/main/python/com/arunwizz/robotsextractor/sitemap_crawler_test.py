'''
Created on 13-May-2012

@author: aruny
'''
import unittest
from com.arunwizz.robotsextractor.sitemap_crawler import RobotsTxtFetcher


class Test(unittest.TestCase):


    def testName(self):
        rft = RobotsTxtFetcher()
        #rft.sitemap_index_parser('http://www.flipkart.com/sitemap/sitemap_index.xml')        
        #rft.sitemap_index_parser('http://www.infibeam.com/sitemap_beauty.xml')        
        rft.sitemap_index_parser('http://www.letsbuy.com/sitemap.xml ')        
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()