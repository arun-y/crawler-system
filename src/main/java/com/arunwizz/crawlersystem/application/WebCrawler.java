package com.arunwizz.crawlersystem.application;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class to be called to begin web crawling
 * 
 * This class will be responsible to initiate all sub-components like crawler
 * manager, network manager, frontier watcher etc.
 * 
 * @author aruny
 * 
 */
public class WebCrawler {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(WebCrawler.class);

	public static void main(String argv[]) throws IOException {
		if (argv.length != 1) {
			LOGGER.error("Usage: {}", "WebClawler seed-file-location");
			System.exit(1);
		}
		String seedFile = argv[0];

		LOGGER.info("Starting frontier writer therad");
		FrontierWriter fw = new FrontierWriter();
		Thread fwt = new Thread(fw, "FrontierWriter");
		fwt.start();
		LOGGER.info("Started frontier writer thread");
		BufferedReader reader = null;
		Thread t = null;
		try {
			reader = new BufferedReader(new FileReader(new File(seedFile)));
			ThreadGroup tg = new ThreadGroup("Crawlerette");
			t = null;
			String seed;
			while ((seed = reader.readLine()) != null) {
				LOGGER.info("Starting crawlerette for " + seed);
				t = new Thread(tg, new WebCrawlerette(fw, seed));
				t.setDaemon(true);
				t.start();
				LOGGER.info("Started crawlerette for " + seed);
			}
			reader.close();
			// wait on tg, the last crawerette will be awaking/notifying
			// tg to main thread
			LOGGER.info("Going to wait till the last crawlerette notifies me");
			synchronized(tg){
				tg.wait();
			}
			fwt.join();//let frontier writer die
			LOGGER.info("Seems all crawlerettes are done, time go forever");
			System.exit(0);

		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			System.exit(1);
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
	}

}
