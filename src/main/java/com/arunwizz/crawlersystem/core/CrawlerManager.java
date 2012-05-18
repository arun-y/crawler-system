package com.arunwizz.crawlersystem.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Currently crawler manager runs as thread, in long term we can run it a
 * separate process which can communicate over tcp or http with frontier watcher
 * or downloader
 * 
 * @author aruny
 * 
 */
public class CrawlerManager implements Runnable {

	private final static Logger LOGGER = LoggerFactory
			.getLogger(CrawlerSystem.class);

	private Object mutex = new Object();

	private PriorityQueue<CrawlingRequestMessage> requestMessageQueue;

	private Queue<String> readyQueue = new LinkedBlockingQueue<String>();
	private CallBackClass callBackClass = new CallBackClass(readyQueue);

	private DelayCallBackQueue<HostDelayedEntry, String> waitQueue = new DelayCallBackQueue<HostDelayedEntry, String>(
			callBackClass);

	private CrawlingRequestMessageHandler crawlingRequestMessageHandler = null;

	public CrawlerManager() throws IOException {
		this.requestMessageQueue = new PriorityQueue<CrawlingRequestMessage>();
		Thread delayCallBackQueueThread = new Thread(waitQueue,
				"Delay Callback Queue Thread");
		delayCallBackQueueThread.start();

		crawlingRequestMessageHandler = new CrawlingRequestMessageHandler(
				readyQueue, waitQueue, hostDictionary);
		Thread crawlingRequestMessageHandlerThread = new Thread(
				crawlingRequestMessageHandler, "Request Message Handler Thread");
		crawlingRequestMessageHandlerThread.setDaemon(true);
		crawlingRequestMessageHandlerThread.start();

	}

	public void enqueRequest(CrawlingRequestMessage requestMessage) {
		LOGGER.debug("Received crawling request");
		LOGGER.debug("Putting request into queue");
		synchronized (mutex) {
			requestMessageQueue.add(requestMessage);
			LOGGER.debug("Waking up waiting manager thread, if any");
			mutex.notifyAll();
		}
	}

	public void run() {

		do {
			CrawlingRequestMessage message = requestMessageQueue.poll();
			if (message != null) {
				LOGGER.debug("Recevied message in queue");
				// TODO:synchronous handle, for better through-put can spawn
				// thread
				// then do it in separate class and make it therad safe
				// currently handle method is not thread, as member variables
				// are used
				LOGGER.debug("Going to handle the message received");
				handleMessage(message);
				LOGGER.debug("Message handled");
			} else {
				try {
					LOGGER.trace("Time to go for wait mode, till message received");
					synchronized (mutex) {
						mutex.wait(10000);
					}
					LOGGER.trace("Time to check what received");
				} catch (InterruptedException ie) {
					LOGGER.error(ie.getMessage());
					continue;
				}
			}

		} while (true);

	}

	// ////////////////////////////////////////////////////////////////////////////////////
	// ///////////////////////////////////PRIMARY CRAWLER MANAGER DATA
	// STRUCUTURE//////////
	// ////////////////////////////////////////////////////////////////////////////////////
	/**
	 * dictionary of current hosts being managed by crawler manager
	 */
	private Map<String, LinkedBlockingQueue<URL>> hostDictionary = new HashMap<String, LinkedBlockingQueue<URL>>();

	// ////////////////////////////////////////////////////////////////////////////////////
	// ////////////////////////////////////////////////////////////////////////////////////
	// ////////////////////////////////////////////////////////////////////////////////////

	// this method is not required to be thread safe
	private void handleMessage(CrawlingRequestMessage message) {

		try {
			// TODO: update/log message received
			BufferedReader reader = new BufferedReader(new FileReader(new File(
					message.getContentLocation())));
			String url = null;
			while ((url = reader.readLine()) != null) {
				URL urlObject = null;
				try {
					urlObject = new URL(url);
				} catch (MalformedURLException mue) {
					LOGGER.error(mue.getMessage());
					continue;
				}

				if (isRobotsPass(urlObject)) {
					// TODO: update robots pass
					String hostname = urlObject.getHost();

					try {

						// check if this host entry already exists, if not
						// create it
						if (!hostDictionary.containsKey(hostname)) {
							// new host found, create a new
							// LinkedBlockingQueue
							hostDictionary.put(hostname,
									new LinkedBlockingQueue<URL>());
						}
						// add the url into host queue
						hostDictionary.get(hostname).put(urlObject);
						// mark this host as ready
						synchronized (readyQueue) {
							if (!readyQueue.contains(hostname)) {
								readyQueue.add(hostname);
								readyQueue.notify();
							}
						}
					} catch (InterruptedException e) {
						LOGGER.error(e.getMessage());
						// TODO: log this url into status table
					}

				} else {
					// TODO: log this url into status table
					LOGGER.debug("Robots check failed, ignoring url" + url);
					continue;
				}
			}

		} catch (Exception e) {
			LOGGER.error("Error handling message " + message);
			LOGGER.error(e.getMessage());
		}
	}

	private boolean isRobotsPass(URL url) {
		return true;
	}

}
