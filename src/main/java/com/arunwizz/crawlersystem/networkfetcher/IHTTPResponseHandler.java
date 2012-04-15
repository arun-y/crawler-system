package com.arunwizz.crawlersystem.networkfetcher;

import java.nio.channels.SocketChannel;

public interface IHTTPResponseHandler {

	public void handle(SocketChannel sChannel);

}
