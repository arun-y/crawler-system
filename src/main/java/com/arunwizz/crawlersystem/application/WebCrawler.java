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

	private static Queue<String> downloadedStatusQueue;
	Object lock = new Object();

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

		LOGGER.info("Starting download listner thread");
		Thread downloadStatusListnerThread = new Thread(
				new DownloadStatusListner());
		downloadStatusListnerThread.start();
		LOGGER.info("Started download listner thread");

		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(new File(seedFile)));
			downloadedStatusQueue = new LinkedList<String>();
			WebCrawlerette crawertte;
			ThreadGroup tg = new ThreadGroup("Crawlerette");
			Thread t;
			String seed;
			while ((seed = reader.readLine()) != null) {
				LOGGER.info("Starting crawlerette for " + seed);
				crawertte = new WebCrawlerette(fw, seed, downloadedStatusQueue,
						lock);
				t = new Thread(tg, crawertte);
				t.start();
				LOGGER.info("Started crawlerette for " + seed);
			}
			reader.close();

			// wait on tg, the last crawerette will be awaking/notifying
			// tg to main thread
			LOGGER.info("Going to wait till the last crawlerette notifies me");
			synchronized (tg) {
				tg.wait();
			}
			fwt.join();// let frontier writer die
			LOGGER.info("Seems all crawlerettes are done, time go forever");
			System.exit(0);

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

						reader = new BufferedReader(
								new InputStreamReader(cSocket.getInputStream()));
						synchronized (lock) {
							String message = reader.readLine();
							LOGGER.debug("Received Message {}", message);
							downloadedStatusQueue.add(message);
							lock.notifyAll();
						}
						reader.close();
						cSocket.close();// FIXME: will it close reader as well
						LOGGER.trace("Informed crawlerette");

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
