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
import java.util.PriorityQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CrawlerManager implements Runnable {

	private PriorityQueue<RequestMessage> requestMessageQueue;

	public CrawlerManager() {
		requestMessageQueue = new PriorityQueue<RequestMessage>();
	}


	public void enqueRequest(RequestMessage requestMessage) {
		requestMessageQueue.add(requestMessage);
	}
	
	public void run() {

		do {
			RequestMessage message = requestMessageQueue.poll();
			if (message != null) {
				handleMessage(message);
			} else {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
					System.err.println(ie.getMessage());
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
	private Map<String, LinkedBlockingQueue<String>> hostDictionary = new HashMap<String, LinkedBlockingQueue<String>>();

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

		private String hostname;
		private long delayTime;
		private long creationTime;

		public HostDelayedEntry(String hostname, long delayTime) {
			this.hostname = hostname;
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

	private PriorityQueue<HostReadyEntry> readyQueue = new PriorityQueue<HostReadyEntry>();

	private DelayQueue<HostDelayedEntry> waitQueue = new DelayQueue<CrawlerManager.HostDelayedEntry>();

	// ////////////////////////////////////////////////////////////////////////////////////
	// ////////////////////////////////////////////////////////////////////////////////////
	// ////////////////////////////////////////////////////////////////////////////////////

	private void handleMessage(RequestMessage message) {

		try {

			// TODO: update message received
			String filename = message.getContentLocation();
			BufferedReader reader = new BufferedReader(new FileReader(new File(
					filename)));
			String url = null;
			long requestNumber = 0;
			while ((url = reader.readLine()) != null) {
				if (isRobotsPass()) {
					// TODO: update robots pass
					String hostipaddress = doDNS(url);//FIXME: can do this in network class
					int domainStartIndex = url.indexOf("://") + 3;
					String hostname = url.substring(domainStartIndex,
							url.indexOf("/", domainStartIndex));

					try {
						// check if this host entry already exists, if not create it
						if (hostDictionary.containsKey(hostname)) {
							// add the url into host queue
							hostDictionary.get(hostname).put(url);
						} else {
							// new host found, create a new LinkedBlockingQueue
							hostDictionary.put(hostname,
									new LinkedBlockingQueue<String>());
							hostDictionary.get(hostname).put(url);
							//mark this host as ready
							readyQueue.add(new HostReadyEntry(hostname,
									requestNumber));
						}
					} catch (InterruptedException e) {
						System.err.println(e.getMessage());
						// TODO: log this url into status table
					}

				} else {
					// TODO: update robots fail
					continue;
				}
			}
			
			//TODO://start from here
			System.out.println("");
			//start polling ready thread and send url from host queue for download
			//upon coming back from download, put the host in wait thread
			//if host is empty delete, delete host entry from host dictionary,
			//if host dictionary is empty, return

		} catch (IOException ioe) {

		}
	}

	private String doDNS(String urlString) throws MalformedURLException {
		String ipaddress = null;
		URL url = new URL(urlString);
		String host = url.getHost();
		// TODO: do lookup
		ipaddress = host;

		return ipaddress;
	}

	private boolean isRobotsPass() {
		return true;
	}

}
