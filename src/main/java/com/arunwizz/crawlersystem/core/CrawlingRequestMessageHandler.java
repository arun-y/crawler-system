package com.arunwizz.crawlersystem.core;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.nio.DefaultHttpClientIODispatch;
import org.apache.http.impl.nio.pool.BasicNIOConnPool;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.nio.protocol.BasicAsyncRequestProducer;
import org.apache.http.nio.protocol.BasicAsyncResponseConsumer;
import org.apache.http.nio.protocol.HttpAsyncRequestExecutor;
import org.apache.http.nio.protocol.HttpAsyncRequester;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOEventDispatch;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.params.SyncBasicHttpParams;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.ImmutableHttpProcessor;
import org.apache.http.protocol.RequestConnControl;
import org.apache.http.protocol.RequestContent;
import org.apache.http.protocol.RequestExpectContinue;
import org.apache.http.protocol.RequestTargetHost;
import org.apache.http.protocol.RequestUserAgent;
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

	private Queue<String> readyQueue;
	private DelayCallBackQueue<HostDelayedEntry, String> waitQueue = null;
	private Map<String, LinkedBlockingQueue<URL>> hostDictionary = null;
	private HttpAsyncRequester requester = null;
	private BasicNIOConnPool pool = null;

	public CrawlingRequestMessageHandler(
			Queue<String> readyQueue,
			DelayCallBackQueue<HostDelayedEntry, String> waitQueue,
			Map<String, LinkedBlockingQueue<URL>> hostDictionary)
			throws IOReactorException {
		this.readyQueue = readyQueue;
		this.waitQueue = waitQueue;
		this.hostDictionary = hostDictionary;

		// HTTP parameters for the client
		HttpParams params = new SyncBasicHttpParams();
		params.setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 5000)
				.setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 5000)
				.setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE,
						8 * 1024)
				.setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, true)
				.setParameter(CoreProtocolPNames.USER_AGENT,
						"CanopusBot/0.1 (Ubuntu 11.10; Linux x86_64)")
				.setParameter("From", "arunwizz@gmail.com");

		// Create HTTP protocol processing chain
		HttpProcessor httpproc = new ImmutableHttpProcessor(
				new HttpRequestInterceptor[] {
						// Use standard client-side protocol interceptors
						new RequestContent(), new RequestTargetHost(),
						new RequestConnControl(), new RequestUserAgent(),
						new RequestExpectContinue() });
		// Create client-side HTTP protocol handler
		HttpAsyncRequestExecutor protocolHandler = new HttpAsyncRequestExecutor();
		// Create client-side I/O event dispatch
		final IOEventDispatch ioEventDispatch = new DefaultHttpClientIODispatch(
				protocolHandler, params);
		// Create client-side I/O reactor
		IOReactorConfig config = new IOReactorConfig();
		config.setIoThreadCount(1);
		final ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor(
				config);
		// Create HTTP connection pool
		pool = new BasicNIOConnPool(ioReactor, params);
		// Limit total number of connections to just two
		pool.setDefaultMaxPerRoute(2);
		pool.setMaxTotal(20);
		// Run the I/O reactor in a separate thread
		Thread t = new Thread(new Runnable() {

			public void run() {
				try {
					// Ready to go!
					ioReactor.execute(ioEventDispatch);
				} catch (InterruptedIOException ex) {
					LOGGER.error("Interrupted");
				} catch (IOException e) {
					LOGGER.error("I/O error: " + e.getMessage());
				}
				LOGGER.info("SocketIO Reactor Shutdown");
			}

		}, "Socket IO Reactor Thread");
		// Start the client thread
		t.start();
		// Create HTTP requester
		requester = new HttpAsyncRequester(httpproc,
				new DefaultConnectionReuseStrategy(), params);

	}

	@Override
	public void run() {
		do {
			try {
				// start polling ready thread and send url from host queue for
				String hostReadyEntry = null;
				synchronized (readyQueue) {
					hostReadyEntry = readyQueue.poll();
				}
				if (hostReadyEntry != null) {
					URL urlObject = hostDictionary.get(
							hostReadyEntry).poll();
					if (urlObject != null) {
						// download
						HttpHost httpHost = new HttpHost(urlObject.getHost(),
								urlObject.getDefaultPort(),
								urlObject.getProtocol());
						// TODO: make is HttpGet once testing is done

						BasicHttpRequest request = new BasicHttpRequest("HEAD",
								urlObject.getPath());
						requester.execute(new BasicAsyncRequestProducer(
								httpHost, request),
								new BasicAsyncResponseConsumer(), pool,
								new BasicHttpContext(),
								new HTTPResponseHandler(httpHost, request));

						// above execute will return immediately, put the host
						// in wait
						// queue
						waitQueue.put(new HostDelayedEntry(hostReadyEntry,
								10000));
						Statistician.hostWaitQueueEnter(
								hostReadyEntry,
								new Date().getTime());
					} else {
						LOGGER.trace("Nothing to crawl for host: "
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
						readyQueue.wait(10000);
					}
					LOGGER.info("Got up to check, if something in ready queue");
					LOGGER.debug("Wait queue Statistics");
					Map<String, Long> hostWaitQueueStatistics = Statistician
							.getHostWaitQueueStatistics();
					for (Entry<String, Long> hostEntry : hostWaitQueueStatistics
							.entrySet()) {
						LOGGER.debug(hostEntry.getKey() + ":"
								+ hostEntry.getValue());
					}
				}

			} catch (InterruptedException e) {
				LOGGER.error("Inturrepted!! " + e.getMessage());
			}
		} while (true);
	}

}
