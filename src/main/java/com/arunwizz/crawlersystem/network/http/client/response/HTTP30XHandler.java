package com.arunwizz.crawlersystem.network.http.client.response;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class HTTP30XHandler implements IHTTPResponseHandler {

	ByteBuffer buf = ByteBuffer.allocateDirect(128);
	
	@Override
	public void handle(SocketChannel sChannel) {

		System.err.println("HTTP301 Handled");
	}

}
