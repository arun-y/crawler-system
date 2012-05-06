package com.arunwizz.crawlersystem.network.http.client.request;

import java.net.MalformedURLException;

public interface IRequestWriter {

	public String get(String urlString) throws MalformedURLException;

}
