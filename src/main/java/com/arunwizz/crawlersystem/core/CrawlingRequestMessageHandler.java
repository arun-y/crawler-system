package com.arunwizz.crawlersystem.core;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.http.HttpHost;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.nio.reactor.IOReactorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arunwizz.crawlersystem.network.NetworkFetcher;

/**
 * This is a consumer thread for readyqueue
 * 
 * @author aruny
 * 
 */
public class CrawlingRequestMessageHandler implements Runnable {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(CrawlingRequestMessageHandler.class);

	private BlockingQueue<String> readyQueue;
	private HostWaitingQueue<HostDelayedEntry> waitQueue = null;
	private Map<String, LinkedBlockingQueue<URL>> hostDictionary = null;

	private long requestCount = 0;
	private NetworkFetcher networkFetcher = null;

	public CrawlingRequestMessageHandler(BlockingQueue<String> readyQueue,
			HostWaitingQueue<HostDelayedEntry> waitQueue,
			Map<String, LinkedBlockingQueue<URL>> hostDictionary)
			throws IOReactorException {
		this.readyQueue = readyQueue;
		this.waitQueue = waitQueue;
		this.hostDictionary = hostDictionary;
		networkFetcher = new NetworkFetcher();
		Thread networkFetcherTherad = new Thread(networkFetcher,
				"Network Fetcher Thread");
		LOGGER.info("Starting Network Manager Thread");
		networkFetcherTherad.start();
	}

	@Override
	public void run() {
		LOGGER.info("Crawling request message handler therad started running");
		do {
			try {
				// start polling ready thread and send url from host queue for
				LOGGER.trace("wait queue before take() - {}", waitQueue);
				LOGGER.trace("ready queue before take() - {}", readyQueue);
				String hostReadyEntry = readyQueue.take();
				LOGGER.trace("wait queue after take() - {}", waitQueue);
				LOGGER.trace("ready queue after take() - {}", readyQueue);
				LOGGER.info("Found host " + hostReadyEntry + " in ready queue");
				LOGGER.info("Look for next pending url for this host");

				//from ready queue.
				URL urlObject = null;
				BlockingQueue<URL> hostPendingUrl = null;
				synchronized (hostDictionary) {
					hostPendingUrl = hostDictionary.get(hostReadyEntry); 
					if (hostPendingUrl != null && hostPendingUrl.isEmpty()) {
						LOGGER.info("No url pending for host {}, removing from host dictionary", hostReadyEntry);
						hostDictionary.remove(hostReadyEntry);
						continue;
					} 
					if (hostPendingUrl == null) {
						LOGGER.warn("This should not happen, host found in ready queue without entry in host dictionary");
						continue;
					}
					urlObject = hostPendingUrl.take();
				}
				LOGGER.info("Found path " + urlObject.getPath() + " on host "
						+ hostReadyEntry);
				// download
				HttpHost httpHost = new HttpHost(urlObject.getHost(),
						urlObject.getDefaultPort(), urlObject.getProtocol());
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
				waitQueue.put(new HostDelayedEntry(hostReadyEntry, 20000));
			} catch (InterruptedException e) {
				LOGGER.error("Interrupted!! - {}", e.getMessage());
			}
		} while (true);
	}

}
