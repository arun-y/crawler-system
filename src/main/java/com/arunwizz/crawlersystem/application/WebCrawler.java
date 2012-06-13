package com.arunwizz.crawlersystem.application;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class to be called to begin web crawling
 * 
 * This class will be responsible to initiate all sub-components like crawler
 * manager, network manager, frontier watcher etc.
 * 
 * @author aruny
 * 
 */
public class WebCrawler {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(WebCrawler.class);

	public static final String CRAWLED_LOCATION = "/data/crawler_system/crawled_host/";
	public static final int LISTENER_SOCKET_PORT = 54030;

	private static Queue<String> newRequestQueue;
	Object newRequestQueueMonitor = new Object();

	public static void main(String argv[]) throws IOException {
		if (argv.length != 1) {
			LOGGER.error("Usage: {}", "WebClawler seed-file-location");
			System.exit(1);
		}
		String seedFile = argv[0];
		WebCrawler crawler = new WebCrawler();
		crawler.start(seedFile);

	}

	private void start(String seedFile) throws IOException {
		LOGGER.info("Starting frontier writer therad");
		FrontierWriter fw = new FrontierWriter();
		Thread fwt = new Thread(fw, "FrontierWriter");
		fwt.start();
		LOGGER.info("Started frontier writer thread");

		LOGGER.info("Starting HTMLPageProcessor thread");
		HTMLPageProcessor hpp = new HTMLPageProcessor(newRequestQueue,
				newRequestQueueMonitor);
		Thread hppt = new Thread(hpp);
		hppt.start();
		LOGGER.info("Started HTMLPageProcessor thread");

		LOGGER.info("Starting download listner thread");
		Thread downloadStatusListnerThread = new Thread(
				new DownloadStatusListner(hpp));
		downloadStatusListnerThread.start();
		LOGGER.info("Started download listner thread");
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(new File(seedFile)));
			newRequestQueue = new LinkedList<String>();
			WebCrawlerette crawertte;
			ThreadGroup tg = new ThreadGroup("Crawlerette");
			Thread t;
			String seed;
			while ((seed = reader.readLine()) != null) {
				LOGGER.info("Starting crawlerette for " + seed);
				crawertte = new WebCrawlerette(fw, seed, newRequestQueue, newRequestQueueMonitor);
				t = new Thread(tg, crawertte);
				t.start();
				LOGGER.info("Started crawlerette for " + seed);
			}
			reader.close();

			do {
				String newRequest = null;
				synchronized (newRequestQueueMonitor) {
					do {
						newRequestQueueMonitor.wait();
						newRequest = newRequestQueue.poll();
					} while (newRequestQueue == null);
				}
				//TODO: inform crawlerette
			} while (true);

		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			System.exit(1);
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
	}

	/**
	 * A listener socket for getting download status
	 * 
	 * @author aruny
	 * 
	 */
	private final class DownloadStatusListner implements Runnable {

		private final Logger LOGGER = LoggerFactory
				.getLogger(DownloadStatusListner.class);

		private final HTMLPageProcessor hpp;

		public DownloadStatusListner(HTMLPageProcessor hpp) {
			this.hpp = hpp;
		}

		@Override
		public void run() {
			ServerSocket sSocket = null;
			Socket cSocket = null;
			try {
				sSocket = new ServerSocket(LISTENER_SOCKET_PORT);
				do {
					BufferedReader reader = null;

					try {
						LOGGER.trace("Wating for download status message");
						cSocket = sSocket.accept();
						LOGGER.trace("Received download status message");

						reader = new BufferedReader(new InputStreamReader(
								cSocket.getInputStream()));
						String message = reader.readLine();
						LOGGER.debug("Received Message {}", message);
						String[] messageSpit = message.split(":");
						if (!"SUCCESS".equals(messageSpit[0])) {
							LOGGER.error("Can't process failed downloaded");
							//TODO: logic to put this request again, can be here
						} else {
							hpp.process(message);
							LOGGER.debug("Put for processing to HTML Processor");
						}

						reader.close();
						cSocket.close();

					} catch (IOException e) {
						LOGGER.error(e.getMessage());
						if (reader != null) {
							try {
								reader.close();
							} catch (IOException e1) {
								LOGGER.error(e1.getMessage());
							}
						}
						if (sSocket != null) {
							try {
								sSocket.close();
							} catch (IOException e2) {
								LOGGER.error(e2.getMessage());
							}
						}
						if (cSocket != null) {
							try {
								cSocket.close();
							} catch (IOException e3) {
								LOGGER.error(e3.getMessage());
							}
						}
					}
				} while (true);
			} catch (IOException e) {
				LOGGER.error(e.getMessage());
			}
		}
	}
}
