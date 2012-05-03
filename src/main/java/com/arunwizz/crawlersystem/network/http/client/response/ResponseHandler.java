package com.arunwizz.crawlersystem.network.http.client.response;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.SelectionKey;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponseHandler implements Runnable {

	private Object mutex = new Object();

	private static final Logger LOGGER = LoggerFactory
			.getLogger(ResponseHandler.class);
	private LinkedBlockingQueue<SelectionKey> responseQueue;
	private HashMap<String, List<String>> responseData;

	public ResponseHandler() {
		responseQueue = new LinkedBlockingQueue<SelectionKey>();
		responseData = new HashMap<String, List<String>>();
	}

	@Override
	public void run() {
		LOGGER.debug("watcher thread started running");
		SelectionKey selectionKey;
		do {
			if ((selectionKey = responseQueue.poll()) != null) {
				LOGGER.trace("response found for handling");
				parse(selectionKey);
			} else {
				try {
					synchronized (mutex) {
						mutex.wait(10000);
					}
					LOGGER.trace("waking up to check if someting arrived");
				} catch (InterruptedException e) {
					LOGGER.error(e.getMessage());
				}
			}
		} while (true);
	}

	private int counter;

	private synchronized int getCounter() {
		return ++counter;
	}

	private class HTTPResponse {
		private HashMap<String, String> header = new HashMap<String, String>();
		private File outFile = null;

		public HTTPResponse() {
		}

		public void setOutFile(File outFile) {
			this.outFile = outFile;
		}

		public File getOutFile() {
			return outFile;
		}

		public void setHeader(String headerKey, String value) {
			header.put(headerKey, value);
		}

		public String getHeader(String headerKey) {
			return header.get(headerKey);
		}
	}

	private HashMap<String, HTTPResponse> responseOut = new HashMap<String, HTTPResponse>();

	private void parse(SelectionKey selectionKey) {

		String url = selectionKey.attachment().toString();
		
		String responseString = null;//read(selectionKey);
		
		LOGGER.trace(responseString);
		int lineStartIndex = 0;
		int lineEndIndex = responseString.indexOf("\r\n");
		try {
			HTTPResponse response = responseOut.get(url);
			if (response == null) {
				// new response
				// read new line

				String responseStatus = responseString.substring(
						lineStartIndex, lineEndIndex);
				if (!responseStatus.contains("200")) {
					LOGGER.error("Error: " + responseStatus);
					return;
				}
				lineStartIndex = lineEndIndex + 2;
				lineEndIndex = responseString.indexOf("\r\n", lineStartIndex);
				String headerString = responseString.substring(lineStartIndex,
						lineEndIndex);
				String[] header = headerString.split(":");
				response = new HTTPResponse();
				response.setHeader(header[0], header[1]);
				while (true) {
					lineStartIndex = lineEndIndex + 2;
					lineEndIndex = responseString.indexOf("\r\n",
							lineStartIndex);
					headerString = responseString.substring(lineStartIndex,
							lineEndIndex);
					header = headerString.split(":");
					if (header.length < 2) {
						break;
					}
					response.setHeader(header[0], header[1]);
				}
				// done parsing all header.

				responseOut.put(url, response);
			}

			// Transfer-Encoding is chunked: handle it
			String responseBody = null;
			String transferEncoding = response.getHeader("Transfer-Encoding").trim();
			if (transferEncoding != null && transferEncoding.equals("chunked")) {
				String chunkSizeOctal = responseString.substring(
						lineStartIndex, lineEndIndex);
				if (chunkSizeOctal.equals("0")) {
					// last chunk
					// close the channel and cancel selection key
				}
				responseBody = responseString.substring(lineEndIndex + 2);
				int chunkSizeDecimal = Integer.parseInt(chunkSizeOctal, 16);
				// FIXME: what if not all bytes received as per chunkSize
			} else {
				responseBody = responseString.substring(lineEndIndex + 2);
			}

			File responseOutFile = response.getOutFile();
			if (responseOutFile == null) {

				URL urlObj = null;
				urlObj = new URL(url);
				File directory = new File("/data/crawler_system/"
						+ urlObj.getHost());
				if (!directory.isDirectory()) {
					directory.mkdir();
				}
				File responseFile = new File(directory.getAbsolutePath() + "/"
						+ getCounter());
				if (responseFile.createNewFile()
						&& !responseBody.trim().equals("")) {
					FileWriter writer = new FileWriter(responseFile);
					writer.write(responseBody);
					writer.flush();
					writer.close();
				}
				response.setOutFile(responseFile);
			} else {
				if (!responseBody.trim().equals("")) {
					FileWriter writer = new FileWriter(responseOutFile, true);
					writer.write(responseBody);
					writer.flush();
					writer.close();
				}

			}

		} catch (MalformedURLException e) {
			LOGGER.error(e.getMessage());
		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		}

		// LOGGER.info("DOWNLOADED :" + url + " " + response.indexOf("200 OK"));
	}

	private void read(SelectionKey selectionKey) {

		
		
	}

	public void handle(SelectionKey selectionKey) {
		LOGGER.debug("adding ready ready selection key into queue");
		synchronized (mutex) {
			responseQueue.add(selectionKey);
			mutex.notify();
		}
		LOGGER.debug("waking up thread to process");
	}

}
