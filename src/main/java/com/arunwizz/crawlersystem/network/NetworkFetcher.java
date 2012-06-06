package com.arunwizz.crawlersystem.network;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arunwizz.crawlersystem.core.HTTPResponseHandler;

/**
 * This is a network interfacing class, the crawler can submit the request to
 * this class queue. It will in-turn create thread for each request from
 * controlled thread queue.
 * 
 * @author aruny
 * 
 */
public class NetworkFetcher implements Runnable {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(NetworkFetcher.class);
	private ReentrantLock lock = new ReentrantLock();
	private Condition newFetchRequestcondition = lock.newCondition();
	private LinkedBlockingQueue<URL> requestQueue;
	private Map<URL, HTTPResponseHandler> requestHandlerMap;

	private static final int NTHREDS = 10;
	private ExecutorService executor;

	public NetworkFetcher() {
		requestQueue = new LinkedBlockingQueue<URL>();
		requestHandlerMap = new HashMap<URL, HTTPResponseHandler>();
		executor = Executors.newFixedThreadPool(NTHREDS);
	}

	public void fetch(HttpHost host, HttpRequest request,
			HTTPResponseHandler httpResponseHandler) {
		lock.lock();
		try {
			URL url = new URL(host.getSchemeName(), host.getHostName(), request
					.getRequestLine().getUri());
			requestQueue.add(url);
			requestHandlerMap.put(url, httpResponseHandler);
			newFetchRequestcondition.signal();
		} catch (MalformedURLException e) {
			LOGGER.info(e.getMessage());
		} finally {
			lock.unlock();
		}

	}

	@Override
	public void run() {
		do {
			LOGGER.info("Looking for queue");
			URL url = null;
			lock.lock();
			try {
				url = requestQueue.poll();
				if (url != null) {
					LOGGER.debug("submitting requesting for " + url);
					executor.submit(new LocalHttpClient(new HttpHost(url
							.getHost()), new BasicHttpRequest("GET", url
							.getPath()), requestHandlerMap.get(url)));

				} else {
					LOGGER.trace("Nothing found, going to sleep");
					newFetchRequestcondition.await(1000, TimeUnit.SECONDS);
				}
			} catch (InterruptedException e) {
				LOGGER.error(e.getMessage());
				requestHandlerMap.get(url).failed(e);
			} finally {
				lock.unlock();
			}
		} while (true);
	}

	private class LocalHttpClient implements Runnable {

		private HttpHost host;
		private HttpRequest request;
		private HTTPResponseHandler httpResponseHandler;
		private HttpClient client;

		public LocalHttpClient(HttpHost host, HttpRequest request,
				HTTPResponseHandler httpResponseHandler) {
			this.host = host;
			this.request = request;
			this.httpResponseHandler = httpResponseHandler;

			client = new DefaultHttpClient();
			HttpParams params = client.getParams();
			// HTTP parameters for the client
			params.setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 5000)
					.setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT,
							500000)
					.setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE,
							8 * 1024)
					.setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, true)
					.setParameter(CoreProtocolPNames.USER_AGENT,
							"CanopusBot/0.1 (Ubuntu 11.10; Linux x86_64)")
					.setParameter("From", "arunwizz@gmail.com");
		}

		@Override
		public void run() {
			HttpResponse response = null;
			try {
				long st = System.currentTimeMillis();
				response = client.execute(host, request);
				if (response != null) {
					httpResponseHandler.completed(response);
				} else {
					httpResponseHandler.failed(new Exception(
							"Error getting response"));
				}
				LOGGER.info(host + " fetched in " + (System.currentTimeMillis() - st) + " ms.");
			} catch (ClientProtocolException e) {
				LOGGER.error(e.getMessage());
				httpResponseHandler.failed(e);
			} catch (IOException e) {
				LOGGER.error(e.getMessage());
				httpResponseHandler.cancelled();
			}
		}

	}

}
