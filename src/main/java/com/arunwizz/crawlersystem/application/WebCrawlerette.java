package com.arunwizz.crawlersystem.application;

import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A WebCrawler Thread, to initiate the crawling request
 * 
 * Each thread will be responsible to a single host. Following activities will
 * be taken care by each therad
 * 
 * 1. Send request for a given host 2. Initiate the Listener thread on
 * /crawled_data/<host_name> 3. Upon receiving any downloaded file in above
 * step, call the page parser 4. clean up the page, fetch the embedded urls and
 * send the request to crawler manager. And then store the cleaned-up page into
 * page storage? (hbase? or BTree?) 5. How will it stop, how will it know that
 * crawling a given host is done?
 * 
 * Also sending new url fetch should go a central FrontierWriter class thread,
 * 
 * @author aruny
 * 
 */
public class WebCrawlerette implements Runnable {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(WebCrawlerette.class);
	private FrontierWriter frontierWriter;
	private String seedUrl;

	private final Queue<String> newRequestQueue;
	private final Object newRequestQueueMonitor;

	public WebCrawlerette(FrontierWriter frontierWriter, String seedUrl,
			Queue<String> requestQueue, Object requestQueueMonitor) {
		this.frontierWriter = frontierWriter;
		this.seedUrl = seedUrl;
		this.newRequestQueue = requestQueue;
		this.newRequestQueueMonitor = requestQueueMonitor;
	}

	@Override
	public void run() {
		try {
			LOGGER.info("Starting to crawl domain with seed " + seedUrl);
			LOGGER.debug("Writing request for {}", seedUrl);
			frontierWriter.write(seedUrl);
			LOGGER.debug("Request sent for {}", seedUrl);
			do {
				// wait for more request from master crawler
				// TODO:
				String requestURL = null;
				// consumer
				synchronized (newRequestQueueMonitor) {
					requestURL = newRequestQueue.poll();
					while (requestURL == null) {
						newRequestQueueMonitor.wait();
						requestURL = newRequestQueue.poll();
					}
				}
				frontierWriter.write(requestURL);
			} while (true);
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		}
	}
}
