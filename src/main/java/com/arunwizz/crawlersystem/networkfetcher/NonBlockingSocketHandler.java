package com.arunwizz.crawlersystem.networkfetcher;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.Iterator;


public class NonBlockingSocketHandler {
	
	public static Charset charset = Charset.forName("UTF-8");
	public static CharsetEncoder encoder = charset.newEncoder();
	public static CharsetDecoder decoder = charset.newDecoder();

	private HTTPResponseHandler responseHandler = new HTTPResponseHandler();
	
	public void run() {

		// Create a selector and register two socket channels
		Selector selector = null;
		try {
			// Create the selector
			selector = Selector.open();

			// Create two non-blocking sockets. This method is implemented in
			// Creating a Non-Blocking Socket.
			SocketChannel sChannel1 = createSocketChannel(
					"www.udacity.com", 80);
			SocketChannel sChannel2 = createSocketChannel(
					"blog.tonycode.com", 80);

			// Register the channel with selector, listening for all events
//			SelectionKey key1 = sChannel1.register(selector,
//					sChannel1.validOps());
//			key1.attach("/index.html");
			SelectionKey key2 = sChannel2.register(selector,
					sChannel1.validOps());
			key2.attach("tech-stuff/http-notes/making-http-requests-via-telnet");
		} catch (IOException e) {
		}

		// Wait for events
		while (true) {
			try {
				// Wait for an event
				selector.select();
			} catch (IOException e) {
				// Handle error with selector
				break;
			}

			// Get list of selection keys with pending events
			Iterator<SelectionKey> it = selector.selectedKeys().iterator();

			// Process each key at a time
			while (it.hasNext()) {
				// Get the selection key
				SelectionKey selKey = it.next();

				// Remove it from the list to indicate that it is being
				// processed
				it.remove();

				try {
					processSelectionKey(selKey);
				} catch (IOException e) {
					// Handle error with channel and unregister
					selKey.cancel();
				}
			}
		}
	}

	private void processSelectionKey(SelectionKey selKey) throws IOException {
		// Since the ready operations are cumulative,
		// need to check readiness for each operation
		if (selKey.isValid() && selKey.isConnectable()) {
			// Get channel with connection request
			SocketChannel sChannel = (SocketChannel) selKey.channel();

			boolean success = sChannel.finishConnect();
			if (!success) {
				// An error occurred; handle it

				// Unregister the channel with this selector
				selKey.cancel();
			}
		}
		if (selKey.isValid() && selKey.isReadable()) {
			// Get channel with bytes to read
			SocketChannel sChannel = (SocketChannel) selKey.channel();
			responseHandler.handle(sChannel);
		
		}
		if (selKey.isValid() && selKey.isWritable()) {
			// Get channel that's ready for more bytes
			SocketChannel sChannel = (SocketChannel) selKey.channel();

			// Create a direct buffer to get bytes from socket.
			// Direct buffers should be long-lived and be reused as much as
			// possible.
			ByteBuffer buf = ByteBuffer.allocateDirect(1024);

			try {
				// Fill the buffer with the bytes to write;
				// see Putting Bytes into a ByteBuffer
				String getRequest = "GET " + selKey.attachment() + " HTTP/1.1\r\n\r\n";
				buf.put(getRequest.getBytes());

				
				// Prepare the buffer for reading by the socket
				buf.flip();

				// Write bytes
				int numBytesWritten = sChannel.write(buf);
				System.out.println(numBytesWritten + " bytes SENT through "
						+ selKey.hashCode() + " - " + getRequest);
				
				selKey.interestOps(SelectionKey.OP_READ);
				
			} catch (IOException e) {
				// Connection may have been closed
			}

			// See Writing to a SocketChannel
		}
	}

	// Creates a non-blocking socket channel for the specified host name and
	// port.
	// connect() is called on the new channel before it is returned.
	private static SocketChannel createSocketChannel(String hostName, int port)
			throws IOException {
		// Create a non-blocking socket channel
		SocketChannel sChannel = SocketChannel.open();
		sChannel.configureBlocking(false);

		// Send a connection request to the server; this method is non-blocking
		sChannel.connect(new InetSocketAddress(hostName, port));
		return sChannel;
	}

}
