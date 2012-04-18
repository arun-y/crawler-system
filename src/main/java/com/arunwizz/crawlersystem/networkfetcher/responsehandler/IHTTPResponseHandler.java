package com.arunwizz.crawlersystem.networkfetcher.responsehandler;

import java.nio.channels.SocketChannel;

import com.arunwizz.crawlersystem.networkfetcher.responseprocessor.HTTPResponse;

public interface IHTTPResponseHandler {

	public void handle(SocketChannel sChannel);

}
