package com.arunwizz.crawlersystem.networkfetcher;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.junit.Test;

import com.arunwizz.crawlersystem.network.http.client.response.IResponseHandler;
import com.arunwizz.crawlersystem.network.http.client.response.ResponseHandler;
import com.arunwizz.crawlersystem.network.tcp.NonBlockingNetworkFetcher;

public class NonBlockingNetworkFetcherTest {

	@Test
	public void testGet() throws IOException, InterruptedException {
		
		ResponseHandler responseHandler = new ResponseHandler();
		Thread t0 = new Thread(responseHandler);
		t0.start();
		
		NonBlockingNetworkFetcher fetcher = new NonBlockingNetworkFetcher(responseHandler);
		Thread t1 = new Thread(fetcher);
		t1.start();
		
		fetcher.get(new URL("http://www.udacity.com:80/cs101x/index.html"));
		
		t1.join();
		
		System.out.println("");
	}

}
