package com.arunwizz.crawlersystem.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.util.logging.resources.logging;

import com.arunwizz.crawlersystem.network.tcp.NonBlockingNetworkFetcher;
import com.arunwizz.crawlersystem.statistics.Statistician;

public class CrawlerManager implements Runnable {

	private final static Logger LOGGER = LoggerFactory
			.getLogger(CrawlerSystem.class);

	private Object mutex = new Object();
	private PriorityQueue<RequestMessage> requestMessageQueue;
	private NonBlockingNetworkFetcher nonBlockingNetworkFetcher;

	private CallBackClass callBackClass = new CallBackClass();
	private DelayCallBackQueue<HostDelayedEntry> waitQueue = new DelayCallBackQueue<HostDelayedEntry>(
			callBackClass);
	private PriorityQueue<HostReadyEntry> readyQueue = new PriorityQueue<HostReadyEntry>();

	public CrawlerManager(NonBlockingNetworkFetcher nonBlockingNetworkFetcher)
			throws IOException {
		this.requestMessageQueue = new PriorityQueue<RequestMessage>();
		this.nonBlockingNetworkFetcher = nonBlockingNetworkFetcher;
		Thread delayCallBackQueueThread = new Thread(waitQueue,
				"Delay Callback Queue Thread");
		delayCallBackQueueThread.start();

	}

	public void enqueRequest(RequestMessage requestMessage) {
		LOGGER.debug("Received crawling request");
		LOGGER.debug("Putting request into queue");
		synchronized (mutex) {
			requestMessageQueue.add(requestMessage);
			LOGGER.debug("Waking up waiting manager thread, if any");
			mutex.notify();
		}
	}

	public void run() {

		do {
			RequestMessage message = requestMessageQueue.poll();
			if (message != null) {
				LOGGER.debug("Recevied message in queue");
				// synchronous handle, for better through-put can spawn thread
				// then do it in separate class and make it therad safe
				// currently handle method id not thread, as member variables
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

	/**
	 * Entry for ready priority queue, entries are ordered by request number
	 * 
	 * @author aruny
	 * 
	 */
	private class HostReadyEntry implements Comparable<HostReadyEntry> {

		private String hostname;
		private long requestNumber;

		public HostReadyEntry(String hostname, long requestNumber) {
			this.hostname = hostname;
			this.requestNumber = requestNumber;
		}

		@Override
		public int compareTo(HostReadyEntry o) {
			if (requestNumber < o.requestNumber) {
				return -1;
			} else if (requestNumber > o.requestNumber) {
				return +1;
			} else {
				return 0;
			}
		}

	}

	/**
	 * Entry for wait queue, entries are ordered by remaining delay time
	 * 
	 * @author aruny
	 * 
	 */
	private class HostDelayedEntry implements Delayed {

		private HostReadyEntry hostReadyEntry;
		private long delayTime;
		private long creationTime;

		public HostDelayedEntry(HostReadyEntry hostReadyEntry, long delayTime) {
			this.hostReadyEntry = hostReadyEntry;
			this.delayTime = delayTime;
			this.creationTime = new Date().getTime();
		}

		@Override
		public int compareTo(Delayed o) {
			if (o instanceof HostDelayedEntry) {

				if (delayTime < ((HostDelayedEntry) o).delayTime) {
					return -1;
				} else if (delayTime > ((HostDelayedEntry) o).delayTime) {
					return +1;
				} else {
					return 0;
				}
			} else {
				throw new ClassCastException();
			}
		}

		@Override
		public long getDelay(TimeUnit unit) {

			long delay = delayTime - (new Date().getTime() - creationTime);

			switch (unit) {
			case DAYS:
				return delay / (24 * 60 * 60 * 1000);
			case HOURS:
				return delay / (60 * 60 * 1000);
			case MINUTES:
				return delay / (60 * 1000);
			case SECONDS:
				return delay / (1000);
			case MICROSECONDS:
				return delay * 10 ^ 3;
			case NANOSECONDS:
				return delay * 10 ^ 6;
			default:
				return delay;
			}
		}

	}

	private class CallBackClass implements ICallBackClass<HostDelayedEntry> {

		@Override
		public void callBack(HostDelayedEntry hostDelayedEntry) {
			Statistician.hostWaitQueueExit(hostDelayedEntry.hostReadyEntry.hostname, new Date().getTime());
			synchronized (readyQueue) {
				readyQueue.add(hostDelayedEntry.hostReadyEntry);
				readyQueue.notify();
			}
		}

	}

	// ////////////////////////////////////////////////////////////////////////////////////
	// ////////////////////////////////////////////////////////////////////////////////////
	// ////////////////////////////////////////////////////////////////////////////////////

	private void handleMessage(RequestMessage message) {
		try {
			// TODO: update/log message received
			String filename = message.getContentLocation();
			BufferedReader reader = new BufferedReader(new FileReader(new File(
					filename)));
			String url = null;
			long requestNumber = 0;
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
					} catch (InterruptedException e) {
						LOGGER.error(e.getMessage());
						// TODO: log this url into status table
					}

				} else {
					LOGGER.debug("Robots check failed, ignoring url" + url);
					continue;
				}
			}

			int crawlCount = 0;
			// Phase 2:
			do {
				// start polling ready thread and send url from host queue for
				HostReadyEntry hostReadyEntry = null;
				while ((hostReadyEntry = readyQueue.poll()) != null) {
					URL urlObject = hostDictionary.get(hostReadyEntry.hostname)
							.poll();
					if (urlObject != null) {
						// download
						nonBlockingNetworkFetcher.get(urlObject);
						crawlCount++;
						// upon coming back from download, put the host in wait
						// thread
						waitQueue
								.put(new HostDelayedEntry(hostReadyEntry, 2000));
						Statistician.hostWaitQueueEnter(hostReadyEntry.hostname, new Date().getTime());
					} else {
						// all urls belonging to this host is done
						// if host is empty delete, delete host entry from host
						// dictionary,
						hostDictionary.remove(hostReadyEntry.hostname);
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
					HashMap<String, Long> hostWaitQueueStatistics = Statistician.getHostWaitQueueStatistics();
					for (Entry<String, Long> hostEntry: hostWaitQueueStatistics.entrySet()) {
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

		} catch (Exception e) {
			LOGGER.error("Error handling message " + message);
			LOGGER.error(e.getMessage());
		}
	}

	private boolean isRobotsPass(URL url) {
		return true;
	}

}
