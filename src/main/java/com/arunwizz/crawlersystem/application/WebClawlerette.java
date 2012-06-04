package com.arunwizz.crawlersystem.application;

/**
 * A WebCrawler Thread, to initiate the crawling request
 * 
 * Each thread will be responsible to a single host.
 * Following activities will be taken care by each therad
 * 
 * 1. Send request for a given host
 * 2. Initiate the Listener thread on /crawled_data/<host_name>
 * 3. Upon receiving any downloaded file in above step, call the page parser
 * 4. clean up the page, fetch the embedded urls and send the request to crawler
 * manager. And then store the cleaned-up page into page storage? (hbase? or BTree?)
 * 5. How will it stop, how will it know that crawling a given host is done?
 * 
 * Also sending new url fetch should go a central FrontierWriter class thread, 
 * 
 * @author aruny
 *
 */
public class WebClawlerette implements Runnable {

	public WebClawlerette(String seedUrl) {
		
	}
	
	@Override
	public void run() {
		

	}

}
