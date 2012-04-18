package com.arunwizz.crawlersystem.networkfetcher;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.arunwizz.crawlersystem.networkfetcher.responseprocessor.HTTPResponse;

public class NonBlockingNetworkFetcher implements Runnable {

	public static Charset charset = Charset.forName("UTF-8");
	public static CharsetEncoder encoder = charset.newEncoder();
	public static CharsetDecoder decoder = charset.newDecoder();

	// Create a selector and register two socket channels
	private Selector selector = null;
	
	public NonBlockingNetworkFetcher() throws IOException {
		selector = Selector.open();
	}

	// Temporary storage for responses
	private Map<SelectionKey, HTTPResponse> responseStorage = new HashMap<SelectionKey, HTTPResponse>();

	private LinkedBlockingQueue<URL> requestQueue = new LinkedBlockingQueue<URL>();
	
	synchronized public void get(URL url) {
			requestQueue.add(url);
			selector.wakeup();

	}

	public void run() {

		try {
			// Wait for events
			while (true) {
				// Wait for an event
				selector.select();
				//check if any new registration
				URL url = null;
				while ((url = requestQueue.poll()) != null) {
					SocketChannel socketChannel = createSocketChannel(url.getHost(),
							url.getPort());
					SelectionKey selectionKey = socketChannel.register(selector,
							socketChannel.validOps());
					selectionKey.attach(url);
				}

				// Get list of selection keys with pending events
				for (SelectionKey selectionKey : selector.selectedKeys()) {
					processSelectionKey(selectionKey);
				}
			}
		} catch (IOException e) {
		}
	}

	private void processSelectionKey(SelectionKey selectionKey)
			throws IOException {
		// Since the ready operations are cumulative,
		// need to check readiness for each operation
		if (selectionKey.isValid() && selectionKey.isConnectable()) {
			// Get channel with connection request
			SocketChannel sChannel = (SocketChannel) selectionKey.channel();

			boolean success = sChannel.finishConnect();
			if (success) {
				// connection established, create storage for this selection key
				responseStorage.put(selectionKey, new HTTPResponse());			
			} else {
				// An error occurred; handle it
				// Unregister the channel with this selector
				selectionKey.cancel();
			}
		}
		if (selectionKey.isValid() && selectionKey.isReadable()) {
			readChannel(selectionKey);
		}
		if (selectionKey.isValid() && selectionKey.isWritable()) {
			writeChannel(selectionKey);
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

	private void writeChannel(SelectionKey selectionKey) {
		// Get channel that's ready for more bytes
		SocketChannel sChannel = (SocketChannel) selectionKey.channel();

		// Create a direct buffer to get bytes from socket.
		// Direct buffers should be long-lived and be reused as much as
		// possible.
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);

		try {
			// Fill the buffer with the bytes to write;
			// see Putting Bytes into a ByteBuffer
			String getRequest = "GET " + ((URL)selectionKey.attachment()).getPath()
					+ " HTTP/1.1\r\n\r\n";
			buf.put(getRequest.getBytes());

			// Prepare the buffer for reading by the socket
			buf.flip();

			// Write bytes
			int numBytesWritten = sChannel.write(buf);
			System.out.println(numBytesWritten + " bytes SENT through "
					+ selectionKey.hashCode() + " - " + getRequest);

			selectionKey.interestOps(SelectionKey.OP_READ);

		} catch (IOException e) {
			// Connection may have been closed
		}
	}

	private ByteBuffer buf = ByteBuffer.allocateDirect(128);

	public void readChannel(SelectionKey selectionKey) {

		SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
		try {
			// Clear the buffer and read bytes from socket
			buf.clear();
			while (socketChannel.read(buf) > 0) {
				// To read the bytes, flip the buffer
				buf.flip();

				 System.out.println(buf.limit() + "bytes RECV: through " +
				 selectionKey.hashCode() + " - " + decoder.decode(buf).toString());
				responseStorage.get(selectionKey).append(decoder.decode(buf));
				buf.clear();
			}

		} catch (IOException e) {
			// Connection may have been closed
			System.err.println(e.getMessage());
			// TODO: log it s
		}

	}

}
