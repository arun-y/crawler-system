package com.arunwizz.crawlersystem.network.http.client.response;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponseHandler implements Runnable {

	private Object mutex = new Object();

	private static final Logger LOGGER = LoggerFactory
			.getLogger(ResponseHandler.class);
	private LinkedBlockingQueue<String> responseQueue;
	private HashMap<String, String> responseData;

	public ResponseHandler() {
		responseQueue = new LinkedBlockingQueue<String>();
		responseData = new HashMap<String, String>();
	}

	@Override
	public void run() {
		LOGGER.debug("watcher thread started running");
		String url;
		do {
			if ((url = responseQueue.poll()) != null) {
				LOGGER.trace("response found for handling");
				parse(url);
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
		
		public HTTPResponse(File outFile) {
			this.outFile = outFile;
		} 
		
		public void setHeader (String headerKey, String value) {
			header.put(headerKey, value);
		}
	}
	
	private HashMap<String, HTTPResponse> responseOutFile = new HashMap<String, HTTPResponse>();

	private void parse(String url) {

		String responseString = responseData.get(url);
		try {
			HTTPResponse response = responseOutFile.get(url);
			if (response == null) {
				// based on response code, for 200 OK, save it to disk, else got
				// the message
				int lineStartIndex = 0;
				int lineEndIndex = responseString.indexOf("\r\n");
				String headerString = responseString.substring(lineStartIndex, lineEndIndex);
				String [] header = headerString.split(":");
				response.setHeader(header[0], header[1]);
				while (header.length == 2) {
					lineStartIndex = lineEndIndex + 2;
					lineEndIndex = responseString.indexOf("\r\n", lineStartIndex);
					headerString = responseString.substring(lineStartIndex, lineEndIndex);
					header = headerString.split(":");
					response.setHeader(header[0], header[1]);
				} 
				//need to parse the header first and store it, based on header need to write remaining
				//file, based transport -encoding chunk header
				int responseHeaderEndIndex = responseString.indexOf("\r\n\r\n");
				// parse header:
				String responseHeader = responseString.substring(0,
						responseHeaderEndIndex);
				String responseBody = responseString
						.substring(responseHeaderEndIndex + 4);

				URL urlObj = null;
				urlObj = new URL(url);
				File directory = new File("/data/crawler_system/"
						+ urlObj.getHost());
				if (!directory.isDirectory()) {
					directory.mkdir();
				}
				responseFile = new File(directory.getAbsolutePath() + "/"
						+ getCounter());
				if (responseFile.createNewFile()
						&& !responseBody.trim().equals("")) {
					FileWriter writer = new FileWriter(responseFile);
					writer.write(responseBody);
					writer.flush();
					writer.close();
				}
				responseOutFile.put(url, responseFile);
			}
			FileWriter writer = new FileWriter(responseFile, true);
			writer.write(responseString);
			

		} catch (MalformedURLException e) {
			LOGGER.error(e.getMessage());
		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		}

		//LOGGER.info("DOWNLOADED :" + url + " " + response.indexOf("200 OK"));
	}

	public void handle(String url, String responseString) {
		LOGGER.debug("adding response to queue");
		synchronized (mutex) {
			responseData.put(url, responseString);
			responseQueue.add(url);
			mutex.notify();
		}
		LOGGER.debug("waking up thread to process");
	}

}
