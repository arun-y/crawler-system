package com.arunwizz.crawlersystem.networkfetcher.responseprocessor;

import java.nio.CharBuffer;

public class HTTPResponse {
	
	private int responseStatusCode;
	private StringBuilder responseStringBuilder;
	
	public HTTPResponse() {
		responseStringBuilder = new StringBuilder();
	}
	
	public int getResponseCode() {
		return responseStatusCode;
	}
	
	public void setResponseCode(int responseStatusCode) {
		this.responseStatusCode = responseStatusCode;
	}
	
	public String getResponseString() {
		return responseStringBuilder.toString();
	}
	
	public void append(CharBuffer content) {
		responseStringBuilder.append(content);
	}

}
