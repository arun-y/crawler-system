package com.arunwizz.crawlersystem.requestfilewatcher;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import com.arunwizz.crawlersystem.core.CrawlerManager;
import com.arunwizz.crawlersystem.core.RequestMessage;

public class FrontierWatcher implements Runnable {

	private WatchService watcher;
	private String frontierPathURI;
	private CrawlerManager crawlerManager;
	

	public FrontierWatcher(String frontierPathURI, CrawlerManager crawlerManager) {
		this.frontierPathURI = frontierPathURI;
		this.crawlerManager = crawlerManager;
	}

	public void run() {

		Path frontierPath = Paths.get(frontierPathURI);
		try {
			watcher = FileSystems.getDefault().newWatchService();
			frontierPath.register(watcher, ENTRY_CREATE);

		do {

			// wait for key to be signaled
			WatchKey key;
			try {
				key = watcher.take();
			} catch (InterruptedException x) {
				return;
			}

			for (WatchEvent<?> event : key.pollEvents()) {
				WatchEvent.Kind<?> kind = event.kind();

				if (kind == OVERFLOW) {
					continue;
				}

				WatchEvent<Path> ev = (WatchEvent<Path>) event;
				Path filename = ev.context();

				if (filename.toString().endsWith(".ready")) {
					
		            Path absolutePath = frontierPath.resolve(filename);
		            String absolutePathString = absolutePath.toString();

					
					RequestMessage requestMessage = new RequestMessage();
					requestMessage.setPriority(0);
					requestMessage.setContentLocation(absolutePathString.substring(0, absolutePathString.indexOf(".ready")));
					//FIXME: In distributed system, this could be TCP/RPC call
					crawlerManager.enqueRequest(requestMessage);
					System.out.println("READY: " + absolutePath.toString());
					// delete the .ready file
					absolutePath.toFile().delete();
				} else {
					continue;
				}

			}

			// Reset the key -- this step is critical if you want to
			// receive further watch events. If the key is no longer valid,
			// the directory is inaccessible so exit the loop.
			boolean valid = key.reset();
			if (!valid) {
				break;
			}

		} while (true);

		} catch (IOException e) {
			System.err.println(e.getMessage());
			//TODO: log the status in db
		}
	}
}
