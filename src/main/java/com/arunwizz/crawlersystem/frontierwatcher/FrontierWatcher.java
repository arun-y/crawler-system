package com.arunwizz.crawlersystem.frontierwatcher;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arunwizz.crawlersystem.core.CrawlerManager;
import com.arunwizz.crawlersystem.core.CrawlingRequestMessage;

public class FrontierWatcher implements Runnable {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(FrontierWatcher.class);

	private WatchService watcher;
	private String frontierPathURI;
	private CrawlerManager crawlerManager;

	public FrontierWatcher(String frontierPathURI, CrawlerManager crawlerManager) {
		this.frontierPathURI = frontierPathURI;
		this.crawlerManager = crawlerManager;
	}

	public void run() {
		Path frontierPath = Paths.get(frontierPathURI);
		LOGGER.debug("Watching path " + frontierPathURI);
		try {
			watcher = FileSystems.getDefault().newWatchService();
			frontierPath.register(watcher, ENTRY_CREATE);
			do {
				// wait for key to be signaled
				WatchKey key;
				try {
					LOGGER.trace("Waiting for some file creating event");
					key = watcher.take();
					LOGGER.debug("Received file creation event");
				} catch (InterruptedException ie) {
					LOGGER.error(ie.getMessage());
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
						LOGGER.debug("Found file " + filename);
						Path absolutePath = frontierPath.resolve(filename);
						String absolutePathString = absolutePath.toString();

						CrawlingRequestMessage requestMessage = new CrawlingRequestMessage();
						requestMessage.setPriority(0);
						requestMessage.setContentLocation(absolutePathString
								.substring(0,
										absolutePathString.indexOf(".ready")));
						// FIXME: In distributed system, this could be TCP/RPC
						// call
						LOGGER.debug("enquing request to crawler manager");
						crawlerManager.enqueRequest(requestMessage);
						// delete the .ready file
						boolean deleted = absolutePath.toFile().delete();
						if (deleted) {
							LOGGER.trace("successfully deleted file "
									+ filename);
						} else {
							LOGGER.trace("failed deleting file " + filename);
						}
					} else {
						LOGGER.trace("Non .ready file, ignore and continue");
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
			LOGGER.error(e.getMessage());
		}
	}
}
