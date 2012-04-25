package com.arunwizz.crawlersystem.network.http.client.response;

public interface IResponseHandler extends Runnable {
	
	public void handle(String url, String responseString);

}
