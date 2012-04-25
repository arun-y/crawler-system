package com.arunwizz.crawlersystem.network.tcp;

import java.net.MalformedURLException;

public interface RequestWriter {

	public String get(String urlString) throws MalformedURLException;

}
