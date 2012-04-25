package com.arunwizz.crawlersystem.network.http.client.response;

import java.nio.channels.SocketChannel;


public interface IHTTPResponseHandler {

	public void handle(SocketChannel sChannel);

}
