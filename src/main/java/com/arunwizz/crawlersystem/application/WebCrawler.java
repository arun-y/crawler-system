package com.arunwizz.crawlersystem.application;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class to be called to begin web crawling
 * 
 * This class will be responsible to initiate all sub-components
 * like crawler manager, network manager, frontier watcher etc.
 * @author aruny
 *
 */
public class WebCrawler {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(WebCrawler.class);
	
	public static void main(String argv[]) {
		if (argv.length != 1) {
			LOGGER.error("Usage: {}", "WebClawler seed-file-location");
			System.exit(1);
		}
		String seedFile = argv[0];
		
		
		
	}

}
