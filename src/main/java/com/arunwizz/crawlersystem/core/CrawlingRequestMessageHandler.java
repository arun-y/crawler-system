package com.arunwizz.crawlersystem.core;

import java.net.URISyntaxException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.xml.crypto.URIReferenceException;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.impl.nio.client.DefaultHttpAsyncClient;
import org.apache.http.nio.client.HttpAsyncClient;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.params.CoreConnectionPNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private PriorityQueue<HostReadyEntry> readyQueue;
	private DelayCallBackQueue<HostDelayedEntry, HostReadyEntry> waitQueue = null;
	private Map<String, LinkedBlockingQueue<URL>> hostDictionary = null;
	private HttpAsyncClient httpclient = null;

	public CrawlingRequestMessageHandler(
			PriorityQueue<HostReadyEntry> readyQueue,
			DelayCallBackQueue<HostDelayedEntry, HostReadyEntry> waitQueue,
			Map<String, LinkedBlockingQueue<URL>> hostDictionary)
			throws IOReactorException {
		this.readyQueue = readyQueue;
		this.waitQueue = waitQueue;
		this.hostDictionary = hostDictionary;
		this.httpclient = new DefaultHttpAsyncClient();
		this.httpclient
				.getParams()
				.setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 5000)
				.setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 5000)
				.setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE,
						8 * 1024)
				.setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, true);
		this.httpclient.start();
	}

	@Override
	public void run() {
		do {
			try {
				// start polling ready thread and send url from host queue for
				HostReadyEntry hostReadyEntry = null;
				while ((hostReadyEntry = readyQueue.poll()) != null) {
					URL urlObject = hostDictionary.get(
							hostReadyEntry.getHostname()).poll();
					if (urlObject != null) {
						// download
						// TODO: make is HttpGet once testing is done
						HttpHead httpGet = new HttpHead(urlObject.toURI());
						httpGet.setHeader("User-Agent",
								"CanopusBot/0.1 (Ubuntu 11.10; Linux x86_64)");
						httpGet.setHeader("Host", urlObject.getHost());
						httpGet.setHeader("From", "arunwizz@gmail.com");
						httpclient.execute(httpGet, new HTTPResponseHandler(
								httpGet));

						// above execute will return immediately, put the host
						// in wait
						// queue
						waitQueue.put(new HostDelayedEntry(hostReadyEntry,
								10000));
						Statistician.hostWaitQueueEnter(
								hostReadyEntry.getHostname(),
								new Date().getTime());
					} else {
						// all urls belonging to this host is done
						// if host is empty delete, delete host entry from host
						// dictionary,
						// since producer thread can still add records,
						// make it thread safe
						// FIXME: can dictionary be weak hashmap
						synchronized (hostDictionary) {
							hostDictionary.remove(hostReadyEntry.getHostname());
						}
					}
				}

				LOGGER.debug("Wait queue Statistics");
				HashMap<String, Long> hostWaitQueueStatistics = Statistician
						.getHostWaitQueueStatistics();
				for (Entry<String, Long> hostEntry : hostWaitQueueStatistics
						.entrySet()) {
					LOGGER.debug(hostEntry.getKey() + ":" + hostEntry.getValue());
				}

				LOGGER.trace("Time to wait on ready queue, till something comes in");
				synchronized (readyQueue) {
					readyQueue.wait(10000);
					LOGGER.trace("Got up to check, if something in ready queue");
				}

			} catch (URISyntaxException use) {
				LOGGER.error("URISyntaxException!! " + use.getMessage());
			} catch (InterruptedException e) {
				LOGGER.error("Inturrepted!! " + e.getMessage());
			}
		} while (true);
	}
}
