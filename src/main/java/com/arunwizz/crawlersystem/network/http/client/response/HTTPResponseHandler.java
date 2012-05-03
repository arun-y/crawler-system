package com.arunwizz.crawlersystem.network.http.client.response;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTTPResponseHandler implements FutureCallback<HttpResponse> {

	private static final Logger LOGGER = LoggerFactory.getLogger(HTTPResponseHandler.class);
	
	private HttpRequest request;
	public HTTPResponseHandler(HttpRequest request) {
		this.request = request;
	}
	
	private int counter;
	private synchronized int getCounter() {
		return ++counter;
	}	
	
    public void completed(final HttpResponse response) {
        LOGGER.info(request.getRequestLine() + "->" + response.getStatusLine());
        File directory = new File("/data/crawler_system/"
				+ request.getFirstHeader("Host").getValue());
		if (!directory.isDirectory()) {
			directory.mkdir();
		}
		File responseFile = new File(directory.getAbsolutePath() + "/"
				+ getCounter());
		try {
			if (responseFile.createNewFile()
					) {
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
		} catch (IllegalStateException e) {
			LOGGER.error(e.getMessage());
		} catch (FileNotFoundException e) {
			LOGGER.error(e.getMessage());
		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		}
    }

    public void failed(final Exception ex) {
    	LOGGER.info(request.getRequestLine() + "->" + ex);
    }

    public void cancelled() {
    	LOGGER.info(request.getRequestLine() + " cancelled");
    }

}
