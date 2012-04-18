package com.arunwizz.crawlersystem.core;

import com.arunwizz.crawlersystem.requestfilewatcher.FrontierWatcher;

public class CrawlerSystem {
	
	public static void main(String argv[]) throws InterruptedException {
	
		if (argv.length != 1) {
			System.out
					.println("Usage: CrawlerSystem <path-of-frontier>");
			System.exit(1);
		}
		CrawlerManager crawlerManager = new CrawlerManager();
		Thread crawlerManagerThread = new Thread(crawlerManager, "Crawler Manager Thread");
		Thread frontierWatcherThread = new Thread(new FrontierWatcher(argv[0], crawlerManager), "Frontier Watcher Thread");
		
		crawlerManagerThread.start();
		frontierWatcherThread.start();
		
		crawlerManagerThread.join();
		frontierWatcherThread.join();
		
	}

}
