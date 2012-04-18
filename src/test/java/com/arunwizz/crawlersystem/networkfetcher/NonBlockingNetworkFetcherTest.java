package com.arunwizz.crawlersystem.networkfetcher;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.junit.Test;

public class NonBlockingNetworkFetcherTest {

	@Test
	public void testGet() throws IOException, InterruptedException {
		NonBlockingNetworkFetcher fetcher = new NonBlockingNetworkFetcher();
		Thread t1 = new Thread(fetcher);
		t1.start();
		
		fetcher.get(new URL("http://www.udacity.com:80/cs101x/index.html"));
		
		t1.join();
		
		System.out.println("");
	}

}
