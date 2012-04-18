package com.arunwizz.crawlersystem.networkfetcher.responsehandler;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class HTTP40XHandler implements IHTTPResponseHandler {

	ByteBuffer buf = ByteBuffer.allocateDirect(128);
	
	@Override
	public void handle(SocketChannel sChannel) {

		System.err.println("HTTP400 Handled");
	}

}
