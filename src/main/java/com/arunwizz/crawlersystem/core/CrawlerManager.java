package com.arunwizz.crawlersystem.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.DefaultHttpAsyncClient;
import org.apache.http.nio.client.HttpAsyncClient;
import org.apache.http.params.CoreConnectionPNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arunwizz.crawlersystem.statistics.Statistician;

public class CrawlerManager implements Runnable {

	private final static Logger LOGGER = LoggerFactory
			.getLogger(CrawlerSystem.class);

	private Object mutex = new Object();

	private PriorityQueue<CrawlingRequestMessage> requestMessageQueue;
	private HttpAsyncClient httpclient = new DefaultHttpAsyncClient();

	private PriorityQueue<HostReadyEntry> readyQueue = new PriorityQueue<HostReadyEntry>();
	private CallBackClass callBackClass = new CallBackClass(readyQueue);

	private DelayCallBackQueue<HostDelayedEntry, HostReadyEntry> waitQueue = new DelayCallBackQueue<HostDelayedEntry, HostReadyEntry>(
			callBackClass);

	public CrawlerManager() throws IOException {
		this.requestMessageQueue = new PriorityQueue<CrawlingRequestMessage>();
		Thread delayCallBackQueueThread = new Thread(waitQueue,
				"Delay Callback Queue Thread");
		delayCallBackQueueThread.start();

		httpclient
				.getParams()
				.setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 5000)
				.setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 5000)
				.setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE,
						8 * 1024)
				.setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, true);
		httpclient.start();

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

	private void handleMessage(CrawlingRequestMessage message) {

		try {
			// Phase 1:
			handleMessagePhase1(message.getContentLocation());
			// Phase 2:
			handleMessagePhase2();
		} catch (Exception e) {
			LOGGER.error("Error handling message " + message);
			LOGGER.error(e.getMessage());
		}
	}

	private void handleMessagePhase1(final String filename) throws IOException {

		// TODO: update/log message received
		BufferedReader reader = new BufferedReader(new FileReader(new File(
				filename)));
		String url = null;
		long requestNumber = 0;//TODO: If multiple thread comes, each will have its own request number
		// Phase 1:
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
					// This is a critical section, make it thread safe
					synchronized (this) {

						// check if this host entry already exists, if not
						// create it
						if (hostDictionary.containsKey(hostname)) {
							// add the url into host queue
							hostDictionary.get(hostname).put(urlObject);
						} else {
							// new host found, create a new LinkedBlockingQueue
							hostDictionary.put(hostname,
									new LinkedBlockingQueue<URL>());
							hostDictionary.get(hostname).put(urlObject);
							// mark this host as ready
							readyQueue.add(new HostReadyEntry(hostname,
									requestNumber++));
						}
					}
				} catch (InterruptedException e) {
					LOGGER.error(e.getMessage());
					// TODO: log this url into status table
				}

			} else {
				LOGGER.debug("Robots check failed, ignoring url" + url);
				continue;
			}
		}

	}

	private void handleMessagePhase2() throws URISyntaxException {
		int crawlCount = 0;
		// Phase 2:
		do {
			// start polling ready thread and send url from host queue for
			HostReadyEntry hostReadyEntry = null;
			while ((hostReadyEntry = readyQueue.poll()) != null) {
				URL urlObject = hostDictionary
						.get(hostReadyEntry.getHostname()).poll();
				if (urlObject != null) {
					// download
					HttpGet httpGet = new HttpGet(urlObject.toURI());
					httpGet.setHeader("User-Agent",
							"CanopusBot/0.1 (Ubuntu 11.10; Linux x86_64)");
					httpclient.execute(httpGet,
							new HTTPResponseHandler(httpGet));
					httpGet.setHeader("Host", urlObject.getHost());
					httpGet.setHeader("From", "arunwizz@gmail.com");
					crawlCount++;
					// upon coming back from download, put the host in wait
					// thread
					synchronized (this) {
						waitQueue
								.put(new HostDelayedEntry(hostReadyEntry, 2000));
					}
					Statistician.hostWaitQueueEnter(
							hostReadyEntry.getHostname(), new Date().getTime());
				} else {
					// all urls belonging to this host is done
					// if host is empty delete, delete host entry from host
					// dictionary,
					hostDictionary.remove(hostReadyEntry.getHostname());
					// FIXME: can dictionary be weak hashmap
					// if host dictionary is empty, return
					if (hostDictionary.isEmpty()) {
						// nothing to crawl, all done
						break;
					}
				}
			}

			if (hostDictionary.isEmpty()) {
				LOGGER.info("Nothing to crawl all done");
				LOGGER.info("Total " + crawlCount + " items crawled");
				LOGGER.info("Wait queue Statistics");
				HashMap<String, Long> hostWaitQueueStatistics = Statistician
						.getHostWaitQueueStatistics();
				for (Entry<String, Long> hostEntry : hostWaitQueueStatistics
						.entrySet()) {
					LOGGER.info(hostEntry.getKey() + ":" + hostEntry.getValue());
				}
				break;
			}

			try {
				LOGGER.trace("Time to wait on ready queue, till something comes in");
				synchronized (readyQueue) {
					readyQueue.wait(10000);
					LOGGER.trace("Got up to check, if something in ready queue");
				}
			} catch (InterruptedException e) {
				LOGGER.debug("Inturrepted!! " + e.getMessage());
			}

		} while (true);
	}

	private boolean isRobotsPass(URL url) {
		return true;
	}

}
