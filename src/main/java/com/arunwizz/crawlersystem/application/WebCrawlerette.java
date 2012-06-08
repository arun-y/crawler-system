package com.arunwizz.crawlersystem.application;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arunwizz.crawlersystem.utils.CommonUtil;

/**
 * A WebCrawler Thread, to initiate the crawling request
 * 
 * Each thread will be responsible to a single host. Following activities will
 * be taken care by each therad
 * 
 * 1. Send request for a given host 2. Initiate the Listener thread on
 * /crawled_data/<host_name> 3. Upon receiving any downloaded file in above
 * step, call the page parser 4. clean up the page, fetch the embedded urls and
 * send the request to crawler manager. And then store the cleaned-up page into
 * page storage? (hbase? or BTree?) 5. How will it stop, how will it know that
 * crawling a given host is done?
 * 
 * Also sending new url fetch should go a central FrontierWriter class thread,
 * 
 * @author aruny
 * 
 */
public class WebCrawlerette implements Runnable {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(WebCrawlerette.class);
	private FrontierWriter frontierWriter;
	private String seedUrl;
	private Queue<String> downloadStatusQueue;
	
	private HashMap<byte[], String> hostUrlDigestMap = new HashMap<byte[], String>();

	public WebCrawlerette(FrontierWriter frontierWriter, String seedUrl, Queue<String> downloadStatusQueue) {
		this.frontierWriter = frontierWriter;
		this.seedUrl = seedUrl;
		this.downloadStatusQueue = downloadStatusQueue;
	}

	@Override
	public void run() {
		try {
			LOGGER.info("Starting to crawl domain with seed " + seedUrl);
			LOGGER.debug("Writing request for {}", seedUrl);
			frontierWriter.write(seedUrl);
			LOGGER.debug("Request sent for {}", seedUrl);
			byte[] urlMD5Digest = CommonUtil.getMD5EncodedDigest(seedUrl);
			hostUrlDigestMap.put(urlMD5Digest, seedUrl);
			String message = null;
			String[] messageSplit = null;
			do {
				synchronized (downloadStatusQueue) {// acquire the queue monitor
					message = downloadStatusQueue.poll();
					while (message == null) {
						LOGGER.trace("Waiting for download status");
						downloadStatusQueue.wait(10000);
						LOGGER.trace("Getting up to check download status");
						message = downloadStatusQueue.poll();
					}
				}
				LOGGER.trace("Received status {}", message);
				messageSplit = message.split(":");
				String status = messageSplit[0];
				byte[] urlDigest = messageSplit[1].getBytes(Charset
						.forName("UTF-8"));
				LOGGER.debug("Received status for {}",
						hostUrlDigestMap.get(urlDigest));
				if ("200".equals(status)) {
					LOGGER.debug("Download successful {}",
							hostUrlDigestMap.get(urlDigest));
					// read the file, clean, parse html links, save file for
					// future
					// processing
					// TODO:
				}
			} while (true);

		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		}

	}

}
