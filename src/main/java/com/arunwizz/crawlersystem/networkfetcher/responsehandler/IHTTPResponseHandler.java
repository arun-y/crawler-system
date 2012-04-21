package com.arunwizz.crawlersystem.networkfetcher.responsehandler;

import java.nio.channels.SocketChannel;


public interface IHTTPResponseHandler {

	public void handle(SocketChannel sChannel);

}
