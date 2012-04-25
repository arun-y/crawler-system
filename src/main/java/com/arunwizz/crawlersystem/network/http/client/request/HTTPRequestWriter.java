package com.arunwizz.crawlersystem.network.http.client.request;

import java.net.MalformedURLException;
import java.net.URL;

import com.arunwizz.crawlersystem.network.tcp.RequestWriter;

public class HTTPRequestWriter implements RequestWriter{

	private static final String CRLF = "\r\n";
	
	public String get(String urlString) throws MalformedURLException {
		
		// Fill the buffer with the bytes to write;
		// see Putting Bytes into a ByteBuffer
		URL url = new URL(urlString);
		
		String getRequest = 
				"GET " + ((URL) url).getPath() + " HTTP/1.1" + CRLF
				+ "Host: " + ((URL) url).getHost() + CRLF
				+ "User-Agent: CanopusBot/0.1 (Ubuntu 11.10; Linux x86_64)" + CRLF 
				+ "From: arunwizz@gmail.com" + CRLF + CRLF;
		
		return getRequest;
	}

	
}
