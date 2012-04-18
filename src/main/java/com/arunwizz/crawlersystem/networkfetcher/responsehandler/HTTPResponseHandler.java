package com.arunwizz.crawlersystem.networkfetcher.responsehandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import com.arunwizz.crawlersystem.networkfetcher.responseprocessor.HTTPResponse;

public class HTTPResponseHandler implements IHTTPResponseHandler {

	public static Charset charset = Charset.forName("UTF-8");
	public static CharsetEncoder encoder = charset.newEncoder();
	public static CharsetDecoder decoder = charset.newDecoder();
	
	private ByteBuffer buf = ByteBuffer.allocateDirect(128);
	
	@Override
	public void handle(SocketChannel sChannel) {

		try {
			// Clear the buffer and read bytes from socket
			buf.clear();
			while ((sChannel.read(buf)) > 0) {
				// To read the bytes, flip the buffer
				buf.flip();
				
				// Read the bytes from the buffer ...;
				// see Getting Bytes from a ByteBuffer
				//System.out.println(numBytesRead + "bytes RECV: through " + selKey.hashCode() + " - " + decoder.decode(buf).toString());
				System.out.print(decoder.decode(buf).toString());
				buf.clear();
			}
		} catch (IOException e) {
			// Connection may have been closed
		}

	}

}
