package com.arunwizz.crawlersystem.networkfetcher.responseprocessor;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponseHandler implements Runnable {

	private Object mutex = new Object();
	
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ResponseHandler.class);
	private LinkedBlockingQueue<String> responseQueue;
	private HashMap<String, String> responseData;

	public ResponseHandler() {
		responseQueue = new LinkedBlockingQueue<String>();
		responseData = new HashMap<String, String>();
	}

	@Override
	public void run() {
		LOGGER.debug("watcher thread started running");
		String url;
		do {
			if ((url = responseQueue.poll()) != null) {
				LOGGER.trace("response found for handling");
				parse(url);
			} else {
				try {
					synchronized (mutex) {
						mutex.wait(10000);
					}
					LOGGER.trace("waking up to check if someting arrived");
				} catch (InterruptedException e) {
					LOGGER.error(e.getMessage());
				}
			}
		} while (true);
	}

	private void parse(String url) {
		//parse header:
		//based on response code, for 200 OK, save it to disk, else got the message
		//TODO: persist into File system
		String response = responseData.get(url);
		LOGGER.info("DOWNLOADED :" + url + " " + response.indexOf("200 OK"));
	}

	public void handle(String url, String responseString) {
		LOGGER.debug("adding response to queue");
		synchronized (mutex) {
			responseData.put(url, responseString);
			responseQueue.add(url);
			mutex.notify();
		}
		LOGGER.debug("waking up thread to process");
	}

}
