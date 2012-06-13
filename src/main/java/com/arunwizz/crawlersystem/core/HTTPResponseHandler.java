package com.arunwizz.crawlersystem.core;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.Charset;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arunwizz.crawlersystem.application.WebCrawler;
import com.arunwizz.crawlersystem.utils.CommonUtil;

public class HTTPResponseHandler implements FutureCallback<HttpResponse> {

	// TODO: Load this from conf
	private static final String CRAWLER_APP_HOST = "localhost";

	private static final String CRAWLED_HOSTS_PATH = "/data/crawler_system/crawled_hosts";
	private static final Logger LOGGER = LoggerFactory
			.getLogger(HTTPResponseHandler.class);

	private HttpHost httpHost;
	private HttpRequest request;

	public HTTPResponseHandler(HttpHost httpHost, HttpRequest request) {
		this.httpHost = httpHost;
		this.request = request;
	}

	public void completed(final HttpResponse response) {
		LOGGER.info(response.getStatusLine() + " -> [" + httpHost.getHostName()
				+ "] " + request.getRequestLine().getMethod() + " "
				+ request.getRequestLine().getUri());

		File directory = new File(CRAWLED_HOSTS_PATH + "/"
				+ httpHost.getHostName());
		if (!directory.isDirectory()) {
			directory.mkdir();
		}

		// create md5 hashes for both host and path
		byte[] pathKeyDigest = CommonUtil.getMD5EncodedDigest(request
				.getRequestLine().getUri());
		OutputStream os = null;
		InputStream is = null;
		try {
			String fileName = new String(pathKeyDigest,
					Charset.forName("UTF-8"));
			File responseFile = new File(directory.getAbsolutePath() + "/"
					+ fileName);

			if (responseFile.createNewFile()) {
				byte[] iobuf = new byte[1024];
				is = response.getEntity().getContent();
				BufferedInputStream bis = new BufferedInputStream(is);
				os = new FileOutputStream(responseFile);
				int byteCount = 0;
				while ((byteCount = bis.read(iobuf)) != -1) {
					os.write(iobuf, 0, byteCount);
				}
				os.flush();
				os.close();
				is.close();


			}
			// inform crawler
			sendMessageToCrawler("SUCCESS:" + request.getRequestLine().getUri() + ":" + responseFile.getAbsolutePath());
			LOGGER.info(responseFile.getAbsolutePath() + " saved");
		} catch (IllegalStateException e) {
			LOGGER.error(e.getMessage());
		} catch (FileNotFoundException e) {
			LOGGER.error(e.getMessage());
		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		} finally {
			if (os != null) {
				try {
					os.close();
					is.close();
				} catch (IOException e) {
					LOGGER.error(e.getMessage());
				}

			}
		}
	}

	public void failed(final Exception ex) {
		// TODO: based on error type, try put it back in delayed queue.
		// may we also we need to track the status of each get quest into
		// B-Tree persistence storage
		LOGGER.info("[" + httpHost + "]" + request.getRequestLine() + "->" + ex);
		// inform crawler
		sendMessageToCrawler("FAILED-" + ex.getMessage() + ":" + request.getRequestLine().getUri());
	}

	public void cancelled() {
		LOGGER.info("[" + httpHost + "]" + request.getRequestLine()
				+ " cancelled");
		// inform crawler
		sendMessageToCrawler("CANCELLED:" + request.getRequestLine().getUri());
	}

	private void sendMessageToCrawler(String message) {
		Socket cSocket = null;
		try {
			cSocket = new Socket(CRAWLER_APP_HOST,
					WebCrawler.LISTENER_SOCKET_PORT);
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					cSocket.getOutputStream()));
			writer.write(message);
			writer.flush();
			LOGGER.info("Informing crawlerette: {}", message);
			cSocket.close();

		} catch (UnknownHostException e) {
			LOGGER.error(e.getMessage());
		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		} finally {
			if (cSocket != null) {
				try {
					cSocket.close();
				} catch (IOException e) {
					LOGGER.error(e.getMessage());
				}
			}
		}
	}

}
