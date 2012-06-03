package com.arunwizz.crawlersystem.core;

import java.net.URL;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.http.HttpHost;
import org.apache.http.impl.nio.pool.BasicNIOConnPool;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.nio.reactor.IOReactorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arunwizz.crawlersystem.network.NetworkFetcher;
import com.arunwizz.crawlersystem.statistics.Statistician;

/**
 * This is a consumer thread for readyqueue
 * 
 * @author aruny
 * 
 */
public class CrawlingRequestMessageHandler implements Runnable {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(CrawlingRequestMessageHandler.class);

	private Queue<String> readyQueue;
	private DelayCallBackQueue<HostDelayedEntry, String> waitQueue = null;
	private Map<String, LinkedBlockingQueue<URL>> hostDictionary = null;

	private long requestCount = 0;
	private NetworkFetcher networkFetcher = null;

	public CrawlingRequestMessageHandler(Queue<String> readyQueue,
			DelayCallBackQueue<HostDelayedEntry, String> waitQueue,
			Map<String, LinkedBlockingQueue<URL>> hostDictionary)
			throws IOReactorException {
		this.readyQueue = readyQueue;
		this.waitQueue = waitQueue;
		this.hostDictionary = hostDictionary;

		networkFetcher = new NetworkFetcher();
		Thread networkFetcherTherad = new Thread(networkFetcher,
				"Network Fetcher Thread");
		networkFetcherTherad.setDaemon(true);
		LOGGER.info("Starting Network Manager Thread");
		networkFetcherTherad.start();
	}

	@Override
	public void run() {
		LOGGER.info("CrawlingRequestManager Therad started running");
		do {
			try {
				// start polling ready thread and send url from host queue for
				String hostReadyEntry = null;
				synchronized (readyQueue) {
					hostReadyEntry = readyQueue.poll();
				}
				if (hostReadyEntry != null) {
					LOGGER.info("Found host " + hostReadyEntry
							+ " in ready queue");
					URL urlObject = hostDictionary.get(hostReadyEntry).poll();

					LOGGER.info(hostReadyEntry + " host queue size: "
							+ hostDictionary.get(hostReadyEntry).size());

					if (urlObject != null) {
						LOGGER.info("Found path " + urlObject.getPath()
								+ " on host " + hostReadyEntry);
						// download
						HttpHost httpHost = new HttpHost(urlObject.getHost(),
								urlObject.getDefaultPort(),
								urlObject.getProtocol());
						// TODO: make is HttpGet once testing is done

						BasicHttpRequest request = new BasicHttpRequest("HEAD",
								urlObject.getPath());
						LOGGER.info("Calling execute for " + request);
						long exceuteStartTime = System.currentTimeMillis();
						networkFetcher.fetch(httpHost, request,
								new HTTPResponseHandler(httpHost, request));
						LOGGER.info("request sent count " + requestCount++);
						LOGGER.info("Execute completed in "
								+ (System.currentTimeMillis() - exceuteStartTime));
						// above execute will return immediately, put the host
						// in wait
						// queue
						waitQueue
								.put(new HostDelayedEntry(hostReadyEntry, 5000));
						Statistician.hostWaitQueueEnter(hostReadyEntry,
								new Date().getTime());
					} else {
						LOGGER.trace("But nothing to crawl for host: "
								+ hostReadyEntry);
						// all urls belonging to this host is done
						// if host is empty delete, delete host entry from host
						// dictionary,
						// since producer thread can still add records,
						// make it thread safe
						// FIXME: can dictionary be weak hashmap
						// synchronized (hostDictionary) {
						// hostDictionary.remove(hostReadyEntry.getHostname());
						// }
					}
				} else {
					LOGGER.trace("Time to wait on ready queue, till something comes in");
					synchronized (readyQueue) {
						readyQueue.wait(1500);
					}
					LOGGER.debug("Got up to check, if something in ready queue");
					LOGGER.trace("Wait queue Statistics");
					Map<String, Long> hostWaitQueueStatistics = Statistician
							.getHostWaitQueueStatistics();
					for (Entry<String, Long> hostEntry : hostWaitQueueStatistics
							.entrySet()) {
						LOGGER.trace(hostEntry.getKey() + ":"
								+ hostEntry.getValue());
					}
				}

			} catch (InterruptedException e) {
				LOGGER.error("Inturrepted!! " + e.getMessage());
			}
		} while (true);
	}

}
