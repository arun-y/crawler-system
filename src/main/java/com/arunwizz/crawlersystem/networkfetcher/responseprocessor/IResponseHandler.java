package com.arunwizz.crawlersystem.networkfetcher.responseprocessor;

public interface IResponseHandler extends Runnable {
	
	public void handle(String url, String responseString);

}
