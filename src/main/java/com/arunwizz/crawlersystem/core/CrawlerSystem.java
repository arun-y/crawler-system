package com.arunwizz.crawlersystem.core;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arunwizz.crawlersystem.networkfetcher.NonBlockingNetworkFetcher;
import com.arunwizz.crawlersystem.networkfetcher.responseprocessor.ResponseHandler;
import com.arunwizz.crawlersystem.networkfetcher.responseprocessor.IResponseHandler;
import com.arunwizz.crawlersystem.requestfilewatcher.FrontierWatcher;

public class CrawlerSystem {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(CrawlerSystem.class);
	
	public static void main(String argv[]) throws InterruptedException, IOException {
	
		Thread.currentThread().setName("Crawler System");
		
		if (argv.length != 1) {
			LOGGER.error("Usage: CrawlerSystem <path-of-frontier>");
			System.exit(1);
		}
		ResponseHandler responseHandler = new ResponseHandler();
		Thread responseHandlerThread = new Thread(responseHandler);
		
		LOGGER.info("instantiating NonblockingNetowrkFetcher");
		NonBlockingNetworkFetcher nonBlockingNetworkFetcher = new NonBlockingNetworkFetcher(responseHandler);
		Thread nonBlockingNetworkFetcherThread = new Thread(nonBlockingNetworkFetcher, "Nonblocking Network Fetcher");
		
		LOGGER.info("instantiating crawler manager");
		CrawlerManager crawlerManager = new CrawlerManager(nonBlockingNetworkFetcher);
		Thread crawlerManagerThread = new Thread(crawlerManager, "Crawler Manager Thread");
		
		LOGGER.info("instantiating frontier watcher");
		Thread frontierWatcherThread = new Thread(new FrontierWatcher(argv[0], crawlerManager), "Frontier Watcher Thread");
		
		LOGGER.info("Starting response handler");
		responseHandlerThread.start();
		LOGGER.info("Starting NonblockingNetworkFetcher thread");
		nonBlockingNetworkFetcherThread.start();
		LOGGER.info("Starting Crawler manager thread");
		crawlerManagerThread.start();
		LOGGER.info("Starting frontier watcher thread");
		frontierWatcherThread.start();

		LOGGER.info("main thread joining other thread");
		nonBlockingNetworkFetcherThread.join();
		crawlerManagerThread.join();
		frontierWatcherThread.join();
		
	}

}
