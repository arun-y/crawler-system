package com.arunwizz.crawlersystem.application;

import java.io.File;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import com.arunwizz.crawlersystem.application.ds.tree.Tree;
import com.arunwizz.crawlersystem.application.ds.tree.TreeUtil;
import com.arunwizz.crawlersystem.application.pageparser.HTMLParser;

/**
 * This thread will receive a request for page processing 1. It will cleanup the
 * page 2. Remove the unwanted HTML tags 3. Extracts the new HTML links 4. Will
 * save the processed page
 * 
 * @author aruny
 * 
 */
public class HTMLPageProcessor implements Runnable {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(HTMLPageProcessor.class);

	private static final String PROCESSED_PAGE_LOCATION = "/data/crawler_system/processed_page";

	private final Queue<String> requestQueue;
	private final Object requestQueueMonitor;
	private final BlockingQueue<String> toBeParsedFileQueue;

	public HTMLPageProcessor(Queue<String> requestQueue,
			Object requestQueueMonitor) {
		this.requestQueue = requestQueue;
		this.requestQueueMonitor = requestQueueMonitor;

		this.toBeParsedFileQueue = new LinkedBlockingQueue<String>();
	}

	public void process(String toBeParsedFile) {
		toBeParsedFileQueue.add(toBeParsedFile);
	}

	@Override
	public void run() {
		HTMLParser htmlParser = new HTMLParser();
		do {
			try {
				String message = toBeParsedFileQueue.take();
				String[] messageSpit = message.split(":");
				String requestedURL = messageSpit[1];
				LOGGER.debug("Processing response for URL {}", requestedURL);
				Document document = htmlParser.parse(new File(messageSpit[2]));
				TreeUtil tutil = new TreeUtil();
				Tree<String> pageTree = tutil.getTreeFromDOM(document);
				// TODO: save the page finally under processed_page
				// remove the file from crawled_page
				// extract all the links and send to frontier

				String[] embeddedHrefs = new String[] {};

				// producer
				synchronized (requestQueueMonitor) {
					for (String embeddedHref : embeddedHrefs) {
						String requestURL = null;
						if (embeddedHref.startsWith("/")) {
							requestURL = messageSpit[1] + embeddedHref;
						} else if (embeddedHref.startsWith("http")) {
							requestURL = embeddedHref;
						} else {
							continue;
						}
						requestQueue.add(requestURL);
						requestQueueMonitor.notifyAll();
					}
				}
			} catch (InterruptedException e) {
				LOGGER.error(e.getMessage());
			} catch (Exception e) {
				LOGGER.error(e.getMessage());
			}
		} while (true);
	}
}
