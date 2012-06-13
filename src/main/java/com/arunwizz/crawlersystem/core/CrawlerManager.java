package com.arunwizz.crawlersystem.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Currently crawler manager runs as thread, in long term we can run it a
 * separate process which can communicate over tcp or http with frontier watcher
 * or downloader
 * 
 * The two primary ready and wait queue will keep moving host to each other If
 * host is just picked from ready queue, it will be put in wait queue with
 * specified delay Once the specified delay is over, wait will will put it back
 * in ready queue
 * 
 * @author aruny
 * 
 */
public class CrawlerManager implements Runnable {

	private final static Logger LOGGER = LoggerFactory
			.getLogger(CrawlerSystem.class);

	// TODO: to be loaded from configuration
	// maximum number of hosts to be managed by crawler manager
	public final static int MAX_HOST_COUNT = 50;

	/*
	 * ALREADY DOWNLOADED SET
	 */
	private final HashSet<String> alreadyDownloadedSet;
	
	/*
	 * INCOMING REQUEST QUEUE
	 */
	// incoming crawling request message blocking queue
	private PriorityBlockingQueue<CrawlingRequestMessage> requestMessageQueue;
	/*
	 * CRAWLER MANAGER - MAIN DATA-STRUCTURES
	 */
	// Thread-safe unbounded blocking queue
	private BlockingQueue<String> readyQueue;
	// Thread-safe unbounded blocking queue
	private HostWaitingQueue<HostDelayedEntry> waitQueue;
	// Not thread-safe, synchronise the access, dictionary of current hosts
	// being managed by crawler manager
	private HashMap<String, LinkedBlockingQueue<URL>> hostDictionary;

	/*
	 * CRAWLING REQUESt MESSAGE HANDLER
	 */
	private CrawlingRequestMessageHandler crawlingRequestMessageHandler = null;
	/*
	 * ROBOTS CHECKER
	 */
	private RobotsChecker robotsChecker = new RobotsChecker();

	public CrawlerManager() throws IOException {
		this.alreadyDownloadedSet = new HashSet<String>();
		this.requestMessageQueue = new PriorityBlockingQueue<CrawlingRequestMessage>();
		this.readyQueue = new LinkedBlockingQueue<String>();
		this.waitQueue = new HostWaitingQueue<HostDelayedEntry>(readyQueue);
		this.hostDictionary = new HashMap<String, LinkedBlockingQueue<URL>>(
				MAX_HOST_COUNT);

		Thread waitingQueueThread = new Thread(waitQueue,
				"Waiting Queue Thread");
		waitingQueueThread.start();

		crawlingRequestMessageHandler = new CrawlingRequestMessageHandler(
				readyQueue, waitQueue, hostDictionary);
		Thread crawlingRequestMessageHandlerThread = new Thread(
				crawlingRequestMessageHandler, "Request Message Handler Thread");
		crawlingRequestMessageHandlerThread.start();
	}

	public void enqueRequest(CrawlingRequestMessage requestMessage) {
		LOGGER.debug("Received crawling request");
		requestMessageQueue.put(requestMessage);
	}

	public void run() {

		do {
			try {

				CrawlingRequestMessage message = requestMessageQueue.take();
				if (message != null) {
					LOGGER.debug("Recevied message in queue");
					// TODO:synchronous handler, for better through-put can
					// spawn
					// thread
					// then do it in separate class and make it therad safe
					// currently handle method is not thread, as member
					// variables
					// are used
					LOGGER.debug("Going to handle the message received");
					handleMessage(message);
					LOGGER.debug("Message handled");
				}
			} catch (InterruptedException e) {
				LOGGER.error("Interrupted!! - {}", e.getMessage());
			}
		} while (true);

	}

	// this method is not required to be thread safe
	private void handleMessage(CrawlingRequestMessage message) {
		BufferedReader reader = null;
		try {
			// TODO: update/log message received
			reader = new BufferedReader(new FileReader(new File(
					message.getContentLocation())));
			String url = null;
			while ((url = reader.readLine()) != null) {
				URL urlObject = null;
				try {
					urlObject = new URL(url);
				} catch (MalformedURLException mue) {
					LOGGER.error(mue.getMessage());
					LOGGER.error("Ignoring url {}", url);
					continue;
				}

				if (alreadySeen(urlObject)) {
					// TODO: log this url into status table
					LOGGER.debug("Aready downloaded, ignoring url {}",
							urlObject);
					continue;
				}

				if (!robotsChecker.isPass(urlObject)) {
					// TODO: log this url into status table
					LOGGER.debug("Robots check failed, ignoring url" + url);
					continue;
				}
				String hostname = urlObject.getHost();
				try {
					// check if this host entry already exists, if not
					// create it
					synchronized (hostDictionary) {
						if (!hostDictionary.containsKey(hostname)) {
							// new host found, create a new
							// LinkedBlockingQueue
							hostDictionary.put(hostname,
									new LinkedBlockingQueue<URL>());
							// add the url into host queue
							hostDictionary.get(hostname).put(urlObject);
							// mark this host as ready
							if (!readyQueue.contains(hostname)) {
								readyQueue.put(hostname);
							}
						} else {
							// add the url into host queue
							hostDictionary.get(hostname).put(urlObject);
						}
					}
				} catch (InterruptedException e) {
					LOGGER.error(e.getMessage());
					// TODO: log this url into status table
				}
			}
			if (reader != null) {
				reader.close();
				reader = null;
			}

		} catch (Exception e) {
			LOGGER.error("Error handling message " + message);
			LOGGER.error(e.getMessage());
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					LOGGER.error("Error closing request file {}",
							message.getContentLocation());
				}
			}
		}
	}

	private boolean alreadySeen(URL urlObject) {
		// TODO Auto-generated method stub
		return false;
	}
}
