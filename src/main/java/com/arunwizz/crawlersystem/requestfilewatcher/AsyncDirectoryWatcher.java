package com.arunwizz.crawlersystem.requestfilewatcher;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

public class AsyncDirectoryWatcher {

	private WatchService watcher;
	private String directoryPathURI;

	public AsyncDirectoryWatcher(String directoryPathURI) {
		this.directoryPathURI = directoryPathURI;
	}

	private void watch() throws IOException {
		watcher = FileSystems.getDefault().newWatchService();
		Path pathOfDirectoryTobeWatched = Paths.get(directoryPathURI);

		pathOfDirectoryTobeWatched.register(watcher, ENTRY_CREATE);

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
					// TODO: look for registered tcp callback to invoke
					System.out.println("READY: " + filename.toString());
					//delete the .ready file
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

	}

	public static void main(String argv[]) throws Exception {
		if (argv.length != 1) {
			System.out
					.println("Usage: AsyncDirectoryWatcher <path-of-directory>");
			System.exit(1);
		}
		AsyncDirectoryWatcher asyncDirectoryWatcher = new AsyncDirectoryWatcher(argv[0]);
		asyncDirectoryWatcher.watch();
	}

}
