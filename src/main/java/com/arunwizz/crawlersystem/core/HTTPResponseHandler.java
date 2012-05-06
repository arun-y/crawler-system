package com.arunwizz.crawlersystem.core;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTTPResponseHandler implements FutureCallback<HttpResponse> {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(HTTPResponseHandler.class);

	private HttpRequest request;

	public HTTPResponseHandler(HttpRequest request) {
		this.request = request;
	}

	public void completed(final HttpResponse response) {
		LOGGER.info(request.getRequestLine() + "->" + response.getStatusLine());
		URI uri = null;
		try {
			uri = new URI(request.getRequestLine().getUri());
		} catch (URISyntaxException e1) {
			LOGGER.error("Error creating URI for "
					+ request.getRequestLine().getUri());
			LOGGER.error(e1.getMessage());
		}
		String host = uri.getHost();
		String path = uri.getPath();

		File directory = new File("/data/crawler_system/" + host);
		if (!directory.isDirectory()) {
			directory.mkdir();
		}

		LOGGER.info(uri.getHost() + ":" + uri.getPath());
		// create md5 hashes for both host and path

		byte[] pathKeyDigest = getMD5EncodedDigest(path);
		
		File responseFile = new File(directory.getAbsolutePath() + "/"
				+ new String(pathKeyDigest));
		try {
			if (responseFile.createNewFile()) {
				byte[] iobuf = new byte[1024];
				InputStream is = response.getEntity().getContent();
				BufferedInputStream bis = new BufferedInputStream(is);
				OutputStream os = new FileOutputStream(responseFile);
				int byteCount = 0;
				while ((byteCount = bis.read(iobuf)) != -1) {
					os.write(iobuf, 0, byteCount);
				}
				os.flush();
				os.close();
				is.close();
			}
			LOGGER.info(responseFile.getAbsolutePath() + " saved");
		} catch (IllegalStateException e) {
			LOGGER.error(e.getMessage());
		} catch (FileNotFoundException e) {
			LOGGER.error(e.getMessage());
		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		}
	}


	public void failed(final Exception ex) {
		//TODO: based on error type, try put it back in delayed queue.
		//may we also we need to track the status of each get quest into 
		//B-Tree persistence storage
		LOGGER.info(request.getRequestLine() + "->" + ex);
	}

	public void cancelled() {
		LOGGER.info(request.getRequestLine() + " cancelled");
	}

	private byte[] getMD5EncodedDigest(String message) {
		byte[] bytesOfMessage = null;
		MessageDigest md = null;
		try {
			bytesOfMessage = message.getBytes("UTF-8");
			md = MessageDigest.getInstance("MD5");
		} catch (UnsupportedEncodingException e) {
			LOGGER.error(e.getMessage());
		} catch (NoSuchAlgorithmException e) {
			LOGGER.error(e.getMessage());
		}
		byte[] digest = md.digest(bytesOfMessage); 
		return Base64.encodeBase64URLSafe(digest);
	}
}