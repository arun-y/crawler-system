package com.arunwizz.crawlersystem.application;

import java.io.File;
import java.io.FileWriter;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Frontier writer therad, this will receive request for new url request
 * across WebCrawlerette
 * 
 * And will keep building the request file and when file threshold is reached,
 * it will dump the request file into Frontier along with ready file. This class
 * will work in co-ordination with Crawlerette
 * 
 * @author aruny
 * 
 */
public class FrontierWriter implements Runnable {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(FrontierWriter.class);
	private final BlockingDeque<String> requestQueue;

	public FrontierWriter() {
		requestQueue = new LinkedBlockingDeque<String>();
	}

	public void write(String url) throws InterruptedException {
		LOGGER.debug("Received request for URL: " + url);
		requestQueue.put(url);
	}

	private static final short MAX_REQUEST_SIZE = 10;
	private static final String FRONTIER_PATH = "/data/crawler_system/frontier/";
	private int requestNumber = 1;

	@Override
	public void run() {
		FileWriter fileWriter = null;
		try {
			String url = null;
			String REQUEST_NUMBER = null;
			short requestCounter = 0;// to track no. of urls in a given request
										// number
			while (true) {
				url = requestQueue.poll(5, TimeUnit.SECONDS);
				if (url == null) {
					if (fileWriter != null) {
						LOGGER.debug("Nothing received in last 5 secs. Comitting whatever written");
						// means nothing received in queue, after waiting for 20
						// sec.
						// commit whatever it is
						fileWriter.close();
						fileWriter = null;
						File file = new File(FRONTIER_PATH + REQUEST_NUMBER
								+ ".ready");
						LOGGER.info(file.getAbsolutePath());
						boolean s = file.createNewFile();// indicate frontier						
						requestCounter = 0;
					}
					continue;
				} 
				if (requestCounter++ == 0) {
					REQUEST_NUMBER = "REQ_" + requestNumber++;
					fileWriter = new FileWriter(new File(FRONTIER_PATH
							+ REQUEST_NUMBER));
					fileWriter.write(url + "\n");
				} else {
					fileWriter.write(url + "\n");
				}
				if (requestCounter == MAX_REQUEST_SIZE){
					//max size reached, commit current
					fileWriter.close();
					requestCounter = 0;
					fileWriter = null;
					File file = new File(FRONTIER_PATH + REQUEST_NUMBER
							+ ".ready");
					file.createNewFile();// indicate frontier
				} 
			}

		} catch (Exception e) {
			LOGGER.error("Error in thread loop: " + e.getMessage());
		} 

	}

}
